<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 20/05/2014
 * Time: 16:31
 */

namespace Packaged\Mappers;

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
      $mapper->hydrate($data);
      $mapper->setExists(true);
      return $mapper;
    }
  }

  public function hydrate(array $values)
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
    return parent::hydrate($values);
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
      'SELECT * FROM ' . static::getTableName()
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
        if(!static::_handleException($e))
        {
          $retries--;
          if(!$retries)
          {
            throw $e;
          }
        }
      }
    }
    throw new \Exception('Query not successful, but failed to throw exception');
  }

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
    return false;
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
    $whereArray = [];
    foreach($criteria as $k => $v)
    {
      $whereArray[] = '"' . $k . '" = ?';
    }
    $where  = $criteria ? ' WHERE ' . implode(' AND ', $whereArray) : '';
    $orderQ = $order ? ' ORDER BY ' . implode(',', (array)$order) : '';
    $limitQ = $limit ? ' LIMIT ' . $limit : '';
    $result = self::execute(
      'SELECT * FROM ' . static::getTableName() . $where . $orderQ . $limitQ,
      $criteria
    );

    $data = [];
    foreach($result as $row)
    {
      $obj = new static();
      $obj->hydrate($row);
      $data[] = $obj;
    }

    return $data;
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

    $changes  = [];
    $mappings = static::_getMetadata()->fieldMappings;
    foreach($mappings as $map)
    {
      $column           = $map['columnName'];
      $field            = $map['fieldName'];
      $changes[$column] = self::_pack(
        $this->$field,
        static::_getCqlFieldType($map)
      );
    }

    /*if(static::UseWideRows())
    {
      // Column Family
      $key   = $this->id();
      $query = 'BEGIN BATCH' . "\n";
      $args  = [];
      foreach($changes as $k => $v)
      {
        $query .= 'INSERT INTO ' . static::getTableName()
          . ' (key,column1,value) VALUES (?,?,?)' . "\n";
        $args[] = $key;
        $args[] = $k;
        $args[] = $v;
      }
      $query .= 'APPLY BATCH;';
      $return = static::execute($query, $args);
    }*/
    // CQL Table
    $query  = sprintf(
      'INSERT INTO "%s" ("%s") VALUES (%s)',
      static::getTableName(),
      implode('", "', array_keys($changes)),
      implode(',', array_fill(0, count($changes), '?'))
    );
    $return = static::execute($query, $changes);
    $this->setExists(true);
    return $return;
  }

  public function saveAsNew($newKey = null)
  {
    $return = new static();
    foreach($this as $k => $v)
    {
      $return->$k = $v;
    }
    $return->setId($newKey);
    $return->save();
    return $return;
  }

  public function reload()
  {
    $data = self::getData($this->id());
    $this->hydrate($data);
    return $this;
  }

  public function delete()
  {
    $keys = [];
    foreach(self::_getKeyColumns() as $k)
    {
      $keys[] = '"' . $k . '" = ?';
    }
    self::execute(
      'DELETE FROM ' . static::getTableName()
      . ' WHERE ' . implode(' AND ', $keys),
      (array)$this->id()
    );
    $this->setExists(false);
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
      'SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = \'' . $keyspace . '\' AND columnfamily_name = \'' . $table . '\';'
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

      $md      = self::_getMetadata();
      $columns = [];
      foreach($md->fieldMappings as $map)
      {
        $columns[] = static::_getCqlField($map);
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
  }

  const TYPE_INTEGER = 'int';
  const TYPE_BIGINT = 'bigint';
  const TYPE_COUNTER = 'counter';
  const TYPE_DECIMAL = 'decimal';
  const TYPE_DOUBLE = 'double';
  const TYPE_FLOAT = 'float';
  const TYPE_VARCHAR = 'varchar';
  const TYPE_UUID = 'uuid';
  const TYPE_BLOB = 'blob';
  const TYPE_BOOLEAN = 'boolean';
  const TYPE_TIMESTAMP = 'timestamp';

  private static $_cqlTypes = [
    'smallint'  => self::TYPE_INTEGER,
    'integer'   => self::TYPE_INTEGER,
    'bigint'    => self::TYPE_BIGINT,
    'counter'   => self::TYPE_COUNTER,
    'decimal'   => self::TYPE_DECIMAL,
    'double'    => self::TYPE_DOUBLE,
    'float'     => self::TYPE_FLOAT,
    'string'    => self::TYPE_VARCHAR,
    'text'      => self::TYPE_VARCHAR,
    'guid'      => self::TYPE_UUID,
    'binary'    => self::TYPE_BLOB,
    'blob'      => self::TYPE_BLOB,
    'boolean'   => self::TYPE_BOOLEAN,
    'date'      => self::TYPE_TIMESTAMP,
    'datetime'  => self::TYPE_TIMESTAMP,
    'datetimez' => self::TYPE_TIMESTAMP,
    'time'      => self::TYPE_TIMESTAMP,
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

  protected static function _pack($value, $type)
  {
    if($value)
    {
      switch($type)
      {
        case self::TYPE_INTEGER:
          return pack('N', $value);
        case self::TYPE_BIGINT:
        case self::TYPE_TIMESTAMP:
        case self::TYPE_COUNTER:
          return self::_packLong($value);
        case self::TYPE_DOUBLE:
        case self::TYPE_DECIMAL:
          return strrev(pack('d', $value));
        case self::TYPE_FLOAT:
          return strrev(pack('f', $value));
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
        case self::TYPE_INTEGER:
          return pack('N', $data);
        case self::TYPE_BIGINT:
        case self::TYPE_TIMESTAMP:
        case self::TYPE_COUNTER:
          return self::_unpackLong($data);
        case self::TYPE_DOUBLE:
        case self::TYPE_DECIMAL:
          return current(unpack('d', strrev($data)));
        case self::TYPE_FLOAT:
          return current(unpack('f', strrev($data)));
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
