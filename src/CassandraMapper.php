<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 20/05/2014
 * Time: 16:31
 */

namespace Packaged\Mappers;

use Packaged\Mappers\Exceptions\CassandraException;
use Packaged\Mappers\Exceptions\InvalidLoadException;
use Packaged\Mappers\Exceptions\MapperException;

abstract class CassandraMapper extends BaseMapper
{
  const PK_INDEX_NAME = 'primary_key';

  protected static $_queryRetries = 3;
  protected static $_autoTimeFormat = self::AUTOTIME_FORMAT_MILLISECONDS;

  public static function getServiceName()
  {
    return 'cassdb';
  }

  /**
   * Load a mapper with a specific id
   *
   * @param mixed $id
   *
   * @return static
   * @throws Exceptions\InvalidLoadException
   * @throws \Exception
   */
  public static function load($id)
  {
    if($id === null)
    {
      throw new InvalidLoadException('No ID passed to load');
    }
    else
    {
      $data = self::getData($id);
      if(!$data)
      {
        throw new InvalidLoadException('No object found with that ID');
      }
      $mapper = new static();
      $mapper->hydrate($data, true);
      $mapper->setExists(true);
      return $mapper;
    }
  }

  public function hydrate(array $values, $persistent = false)
  {
    if($persistent)
    {
      foreach(static::_getMetadata()->fieldMappings as $map)
      {
        if(isset($values[$map['columnName']]))
        {
          $values[$map['columnName']] = static::_unpack(
            $values[$map['columnName']],
            static::_getCqlFieldType($map)
          );
        }
      }
    }
    return parent::hydrate($values, $persistent);
  }

  public static function getData($id)
  {
    $keys = [];
    foreach(self::_getKeyColumns() as $k)
    {
      $keys[] = '"' . $k . '" = ?';
    }
    $md = static::_getMetadata();
    if(!empty($md->table['indexes'][self::PK_INDEX_NAME]['columns']))
    {
      $clusterKey = $md->table['indexes'][self::PK_INDEX_NAME]['columns'];
      $k          = reset($clusterKey);
      while(count($keys) < count($id) && $k)
      {
        $keys[] = '"' . $k . '" = ?';
        $k      = next($clusterKey);
      }
    }

    $result = self::execute(
      'SELECT * FROM ' . self::_escapeIdentifier(static::getTableName())
      . ' WHERE ' . implode(' AND ', $keys),
      (array)$id
    );
    return reset($result);
  }

  /**
   * @param $query
   * @param $parameters
   *
   * @return array
   * @throws \Exception
   */
  public static function execute($query, array $parameters = [])
  {
    $retries = static::$_queryRetries;
    while($retries)
    {
      try
      {
        $conn = static::getConnection();
        $stmt = $conn->prepare($query);
        return $stmt->execute($parameters);
      }
      catch(\Exception $e)
      {
        if(static::_handleException($e))
        {
          static::getConnection()->disconnect();
          $retries--;
          if(!$retries)
          {
            throw $e;
          }
        }
        else
        {
          throw $e;
        }
      }
    }
    throw new \Exception('Query not successful, but failed to throw exception');
  }

  /**
   * Return true if exception was handled and query should be retried
   *
   * @param \Exception $e
   *
   * @return bool
   * @throws \Exception
   */
  protected static function _handleException(\Exception $e)
  {
    if(strpos(
        $e->getMessage(),
        'unconfigured columnfamily ' . static::getTableName()
      ) === 0
    )
    {
      static::createTable();
      return true;
    }

    if($e instanceof CassandraException
      && strpos($e->getMessage(), 'Index already exists') === 0
    )
    {
      // never retry if index exists
      return false;
    }

    return true;
  }

  /**
   * @param array $criteria
   * @param null  $order
   * @param null  $limit
   * @param null  $offset
   *
   * @return static[]
   */
  public static function loadWhere(
    array $criteria, $order = null, $limit = null, $offset = null
  )
  {
    $where = self::_buildWhere($criteria);

    $orderQ = $order ? ' ORDER BY ' . implode(',', (array)$order) : '';
    $limitQ = $limit ? ' LIMIT ' . $limit : '';
    $result = self::execute(
      'SELECT * FROM ' . self::_escapeIdentifier(static::getTableName())
      . $where['where'] . $orderQ . $limitQ,
      $where['params']
    );

    $data = [];
    foreach($result as $row)
    {
      $obj = new static();
      $obj->hydrate($row, true);
      $obj->setExists(true);
      $data[] = $obj;
    }

    return $data;
  }

  public static function deleteWhere(array $criteria)
  {
    $where = self::_buildWhere($criteria);
    if(($where['where'] == '') || (count($criteria) == 0))
    {
      throw new \Exception('Invalid or empty criteria specified');
    }

    self::execute(
      'DELETE FROM ' . self::_escapeIdentifier(static::getTableName())
      . $where['where'],
      $where['params']
    );
  }

  /**
   * @param array $criteria
   *
   * @return array('where' => string, 'params' => array)
   */
  private static function _buildWhere(array $criteria)
  {
    $whereStr   = '';
    $whereArray = [];
    $params     = [];
    foreach($criteria as $k => $v)
    {
      if(is_array($v))
      {
        $numValues = count($v);
        if($numValues > 0)
        {
          $whereArray[] = '"' . $k . '" IN ('
            . implode(",", array_fill(0, $numValues, '?')) . ')';
          $params       = array_merge($params, $v);
        }
      }
      else
      {
        $whereArray[] = '"' . $k . '" = ?';
        $params[]     = $v;
      }
    }
    if(count($whereArray) > 0)
    {
      $whereStr = ' WHERE ' . implode(' AND ', $whereArray);
    }
    return ['where' => $whereStr, 'params' => $params];
  }

  public function save()
  {
    if($this->exists())
    {
      $this->preUpdate();
    }
    else
    {
      $this->preCreate();
    }

    $changes       = [];
    $changedFields = $this->getChangedFields();
    $map           = static::_getMetadata()->fieldMappings;
    foreach($changedFields as $field => $value)
    {
      if($value !== null)
      {
        $changes[$map[$field]['columnName']] = self::_pack(
          $value,
          static::_getCqlFieldType($map[$field])
        );
      }
    }
    foreach(static::_getKeyFields() as $field)
    {
      $changes[$map[$field]['columnName']] = self::_pack(
        $this->$field,
        static::_getCqlFieldType($map[$field])
      );
      if($changes[$map[$field]['columnName']] === null)
      {
        $changes[$map[$field]['columnName']] = '';
      }
    }

    // CQL Table
    if(count($changes) > 0)
    {
      $query = sprintf(
        'INSERT INTO %s ("%s") VALUES (%s)',
        self::_escapeIdentifier(static::getTableName()),
        implode('", "', array_keys($changes)),
        implode(',', array_fill(0, count($changes), '?'))
      );
      static::execute($query, $changes);
      $this->setExists(true);

      $changesMade = [];
      foreach($changedFields as $field => $value)
      {
        $changesMade[$field]          = [
          'from' => isset($this->_persistedData[$field])
            ? $this->_persistedData[$field] : null,
          'to'   => $value
        ];
        $this->_persistedData[$field] = $value;
      }
      $this->_savedChanges = $changesMade;
      return $changesMade;
    }
    return [];
  }

  public function saveAsNew($newKey = null)
  {
    $new = new static();
    $new->hydrate(call_user_func('get_object_vars', $this));
    $new->setId($newKey);
    $new->save();
    return $new;
  }

  public function reload()
  {
    $data = self::getData($this->id());
    $this->hydrate($data, true);
    $this->clearSavedChanges();
    return $this;
  }

  public function delete()
  {
    if($this->exists())
    {
      $keys = [];
      foreach(self::_getKeyColumns() as $k)
      {
        $keys[] = '"' . $k . '" = ?';
      }
      self::execute(
        'DELETE FROM ' . self::_escapeIdentifier(static::getTableName())
        . ' WHERE ' . implode(' AND ', $keys),
        (array)$this->id()
      );
      $this->setExists(false);
    }
    return $this;
  }

  public function increment($field, $count)
  {
    throw new MapperException(
      'Increment only supported in CounterCassandraMapper'
    );
  }

  public function decrement($field, $count)
  {
    throw new MapperException(
      'Deccrement only supported in CounterCassandraMapper'
    );
  }

  /**
   * @return ThriftConnection
   */
  public static function getConnection()
  {
    return static::getConnectionResolver()->getConnection(
      static::getServiceName()
    );
  }

  protected static function _getKeyColumns()
  {
    $keys = parent::_getKeyColumns();

    $md        = static::_getMetadata();
    $keyFields = isset($md->table['indexes'][self::PK_INDEX_NAME]['columns'])
      ? $md->table['indexes'][self::PK_INDEX_NAME]['columns'] : [];

    foreach($keyFields as $k => $field)
    {
      $keyFields[$k] = $md->columnNames[$field];
    }

    return array_merge($keys, $keyFields);
  }

  protected static function _getKeyFields()
  {
    $keys = parent::_getKeyFields();

    $md        = static::_getMetadata();
    $keyFields = isset($md->table['indexes'][self::PK_INDEX_NAME]['columns'])
      ? $md->table['indexes'][self::PK_INDEX_NAME]['columns'] : [];

    return array_merge($keys, $keyFields);
  }

  public static function createTable()
  {
    $table    = static::getTableName();
    $keyspace = static::getConnection()->getKeyspace();
    if(!static::execute(
      'SELECT * FROM system.schema_columnfamilies WHERE'
      . ' keyspace_name = \'' . $keyspace . '\''
      . ' AND columnfamily_name = \'' . $table . '\';'
    )
    )
    {
      /**
       * Fields that make up the partition key
       */
      $partitionKeyFields = [];
      /**
       * Additional fields that make up the primary key but are not
       * part of the partition key
       */
      $primaryKeyFields = [];

      $docBlocks = static::_getDocBlockProperties();

      $md      = self::_getMetadata();
      $columns = [];
      foreach($md->fieldMappings as $map)
      {
        $columns[] = static::_getCqlField($map)
          . ($docBlocks[$map['fieldName']]->hasTag('static') ? ' static' : '');
      }

      if(!empty($md->identifier))
      {
        $partitionKeyFields = $md->identifier;
        foreach($partitionKeyFields as $k => $v)
        {
          $partitionKeyFields[$k] = static::getColumnName($v);
        }
      }
      if(!empty($md->table['indexes'][self::PK_INDEX_NAME]['columns']))
      {
        $primaryKeyFields = $md->table['indexes'][self::PK_INDEX_NAME]['columns'];
        foreach($primaryKeyFields as $k => $v)
        {
          $primaryKeyFields[$k] = static::getColumnName($v);
        }
      }

      $pkStr = '';
      if(count($partitionKeyFields) > 0)
      {
        $pkStr = '(' . self::_implodeColumns($partitionKeyFields) . ')';
      }
      if(count($primaryKeyFields) > 0)
      {
        if($pkStr != "")
        {
          $pkStr .= ',';
        }
        $pkStr .= self::_implodeColumns(
          array_diff($primaryKeyFields, $partitionKeyFields)
        );
      }
      if($pkStr == '')
      {
        throw new \Exception('Error: No primary key specified');
      }

      $query = 'CREATE TABLE "' . $table . '" ('
        . implode(',', $columns)
        . ', PRIMARY KEY (' . $pkStr . '))';
      self::execute($query);
    }

    $docBlocks = static::_getDocBlockProperties();
    foreach($docBlocks as $property => $block)
    {
      if($block->hasTag('index'))
      {
        try
        {
          // convert property to column
          $column = static::_getFieldMap()[$property];
          static::execute(
            'CREATE INDEX ' . $block->getTag('index', $column)
            . ' on ' . static::getTableName() . ' (' . $column . ')'
          );
        }
        catch(\Exception $e)
        {
          if(strpos($e->getMessage(), 'Index already exists') !== 0)
          {
            throw $e;
          }
        }
      }
    }
  }

  const CASSANDRA_TYPE_INTEGER = 'int';
  const CASSANDRA_TYPE_BIGINT = 'bigint';
  const CASSANDRA_TYPE_COUNTER = 'counter';
  const CASSANDRA_TYPE_DECIMAL = 'decimal';
  const CASSANDRA_TYPE_DOUBLE = 'double';
  const CASSANDRA_TYPE_FLOAT = 'float';
  const CASSANDRA_TYPE_VARCHAR = 'varchar';
  const CASSANDRA_TYPE_UUID = 'uuid';
  const CASSANDRA_TYPE_BLOB = 'blob';
  const CASSANDRA_TYPE_BOOLEAN = 'boolean';
  const CASSANDRA_TYPE_TIMESTAMP = 'timestamp';

  private static $_cqlTypes = [
    'smallint'  => self::CASSANDRA_TYPE_INTEGER,
    'integer'   => self::CASSANDRA_TYPE_INTEGER,
    'bigint'    => self::CASSANDRA_TYPE_BIGINT,
    'counter'   => self::CASSANDRA_TYPE_COUNTER,
    'decimal'   => self::CASSANDRA_TYPE_DECIMAL,
    'double'    => self::CASSANDRA_TYPE_DOUBLE,
    'float'     => self::CASSANDRA_TYPE_FLOAT,
    'string'    => self::CASSANDRA_TYPE_VARCHAR,
    'text'      => self::CASSANDRA_TYPE_VARCHAR,
    'guid'      => self::CASSANDRA_TYPE_UUID,
    'binary'    => self::CASSANDRA_TYPE_BLOB,
    'blob'      => self::CASSANDRA_TYPE_BLOB,
    'boolean'   => self::CASSANDRA_TYPE_BOOLEAN,
    'date'      => self::CASSANDRA_TYPE_TIMESTAMP,
    'datetime'  => self::CASSANDRA_TYPE_TIMESTAMP,
    'datetimez' => self::CASSANDRA_TYPE_TIMESTAMP,
    'time'      => self::CASSANDRA_TYPE_TIMESTAMP,
  ];

  protected static function _getCqlField($map)
  {
    return static::_escapeIdentifier($map['columnName']) . ' '
    . static::_getCqlFieldType($map);
  }

  protected static function _getCqlFieldType($map)
  {
    if(isset(self::$_cqlTypes[$map['type']]))
    {
      $type = self::$_cqlTypes[$map['type']];
    }
    else
    {
      $type = $map['type'];
    }
    return $type;
  }

  protected static function _escapeIdentifier($columnName)
  {
    return '"' . $columnName . '"';
  }

  protected static function _implodeColumns(array $columns, $separator = ',')
  {
    $escapedCols = [];
    foreach($columns as $column)
    {
      $escapedCols[] = self::_escapeIdentifier($column);
    }
    return implode($separator, $escapedCols);
  }

  /**
   * Reverse the supplied string's bytes if the current platform is
   * little-endian
   *
   * @param string &$bin
   *
   * @return string
   */
  private static function _reverseIfLE($bin)
  {
    // TODO: The logic below is incorrect!
    // We always run on LE so just reverse everything for now
    return strrev($bin);
    /*static $isBigEndian = null;
    if($isBigEndian === null)
    {
      $isBigEndian = unpack('v', pack('S', 256)) == 256;
    }
    return $isBigEndian ? strrev($bin) : $bin;*/
  }

  protected static function _pack($value, $type)
  {
    if($value !== null && $value !== '')
    {
      switch($type)
      {
        case self::CASSANDRA_TYPE_INTEGER:
          return pack('N', $value);
        case self::CASSANDRA_TYPE_BIGINT:
        case self::CASSANDRA_TYPE_TIMESTAMP:
        case self::CASSANDRA_TYPE_COUNTER:
          return self::_packLong($value);
        case self::CASSANDRA_TYPE_DOUBLE:
        case self::CASSANDRA_TYPE_DECIMAL:
          return self::_reverseIfLE(pack('d', $value));
        case self::CASSANDRA_TYPE_FLOAT:
          return self::_reverseIfLE(pack('f', $value));
        default:
          return $value;
      }
    }
    return $value;
  }

  protected static function _unpack($data, $type)
  {
    if($data)
    {
      switch($type)
      {
        case self::CASSANDRA_TYPE_INTEGER:
          return current(unpack('l', self::_reverseIfLE($data)));
        case self::CASSANDRA_TYPE_BIGINT:
        case self::CASSANDRA_TYPE_TIMESTAMP:
        case self::CASSANDRA_TYPE_COUNTER:
          return self::_unpackLong($data);
        case self::CASSANDRA_TYPE_DOUBLE:
        case self::CASSANDRA_TYPE_DECIMAL:
          return current(unpack('d', self::_reverseIfLE($data)));
        case self::CASSANDRA_TYPE_FLOAT:
          return current(unpack('f', self::_reverseIfLE($data)));
        default:
          return $data;
      }
    }
    return $data;
  }

  protected static function _packLong($value)
  {
    // If we are on a 32bit architecture we have to explicitly deal with
    // 64-bit twos-complement arithmetic since PHP wants to treat all ints
    // as signed and any int over 2^31 - 1 as a float
    if(PHP_INT_SIZE == 4)
    {
      $neg = $value < 0;

      if($neg)
      {
        $value *= -1;
      }

      $hi = (int)($value / 4294967296);
      $lo = (int)$value;

      if($neg)
      {
        $hi = ~$hi;
        $lo = ~$lo;
        if(($lo & (int)0xffffffff) == (int)0xffffffff)
        {
          $lo = 0;
          $hi++;
        }
        else
        {
          $lo++;
        }
      }
      $data = pack('N2', $hi, $lo);
    }
    else
    {
      $hi   = $value >> 32;
      $lo   = $value & 0xFFFFFFFF;
      $data = pack('N2', $hi, $lo);
    }
    return $data;
  }

  protected static function _unpackLong($data)
  {
    $arr = unpack('N2', $data);

    // If we are on a 32bit architecture we have to explicitly deal with
    // 64-bit twos-complement arithmetic since PHP wants to treat all ints
    // as signed and any int over 2^31 - 1 as a float
    if(PHP_INT_SIZE == 4)
    {

      $hi    = $arr[1];
      $lo    = $arr[2];
      $isNeg = $hi < 0;

      // Check for a negative
      if($isNeg)
      {
        $hi = ~$hi & (int)0xffffffff;
        $lo = ~$lo & (int)0xffffffff;

        if($lo == (int)0xffffffff)
        {
          $hi++;
          $lo = 0;
        }
        else
        {
          $lo++;
        }
      }

      // Force 32bit words in excess of 2G to pe positive - we deal wigh sign
      // explicitly below

      if($hi & (int)0x80000000)
      {
        $hi &= (int)0x7fffffff;
        $hi += 0x80000000;
      }

      if($lo & (int)0x80000000)
      {
        $lo &= (int)0x7fffffff;
        $lo += 0x80000000;
      }

      $value = $hi * 4294967296 + $lo;

      if($isNeg)
      {
        $value = 0 - $value;
      }
    }
    else
    {
      // Upcast negatives in LSB bit
      if($arr[2] & 0x80000000)
      {
        $arr[2] = $arr[2] & 0xffffffff;
      }

      // Check for a negative
      if($arr[1] & 0x80000000)
      {
        $arr[1] = $arr[1] & 0xffffffff;
        $arr[1] = $arr[1] ^ 0xffffffff;
        $arr[2] = $arr[2] ^ 0xffffffff;
        $value  = 0 - $arr[1] * 4294967296 - $arr[2] - 1;
      }
      else
      {
        $value = $arr[1] * 4294967296 + $arr[2];
      }
    }
    return $value;
  }
}
