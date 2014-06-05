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
      if(self::_mustPack($map)
        && isset($values[$map['columnName']]) && $values[$map['columnName']]
      )
      {
        $values[$map['columnName']] = self::_unpack(
          $values[$map['columnName']]
        );
      }
    }
    return parent::hydrate($values);
  }

  /**
   * @param $id
   *
   * @return static
   */
  public static function loadOrNew($id)
  {
    try
    {
      $mapper = static::load($id);
    }
    catch(\Exception $e)
    {
      $mapper = new static();
      $mapper->setId($id);
    }
    return $mapper;
  }

  public static function getData($id)
  {
    $keys = [];
    foreach(self::_getKeys() as $k)
    {
      $keys[] = '"' . $k . '" = ?';
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
   * @return IMapper[]
   */
  public static function loadWhere(
    array $criteria, $order = null, $limit = null, $offset = null
  )
  {
    throw new InvalidLoadException('loadWhere is not currently supported');
    /*
      $where  = $criteria ? ' WHERE ' . implode(' AND ', $criteria) : '';
      $result = self::execute(
        'SELECT * FROM ' . static::getTableName() . $where
      );

      $data = [];
      foreach($result as $row)
      {
        if(isset($row['key'])
          && isset($row['column1'])
          && isset($row['value'])
        )
        { // for dynamic tables, column# => value
          $data[$row['key']][$row['column1']] = $row['value'];
        }
        else
        {
          $data[] = $row;
        }
      }
      var_dump($data);
      foreach($data as $k => $v)
      {
        $data[$k]     = new static();
        $data[$k]->id = $k;
        $data[$k]->hydrate($v);
      }

      return $data;
    */
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
      $changes[$column] = self::_mustPack($map)
        ? self::_pack($this->$field) : $this->$field;
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
      "INSERT INTO %s (%s) VALUES (%s)",
      static::getTableName(),
      implode(', ', array_keys($changes)),
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
    foreach(self::_getKeys() as $k)
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

  public function id()
  {
    return $this->_getKeyValues();
  }

  public function setId($value)
  {
    foreach(array_combine(static::_getKeys(), (array)$value) as $k => $v)
    {
      $this->$k = $v;
    }
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

  public static function createTable()
  {
    $table = static::getTableName();
    if(!static::execute(
      'SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = \'Cubex\' AND columnfamily_name = \'' . $table . '\';'
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
      }
      if(!empty($md->table['indexes'][self::PK_INDEX_NAME]['columns']))
      {
        $primaryKeyFields = $md->table['indexes'][self::PK_INDEX_NAME]['columns'];
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

  private static $_cqlTypes = [
    'smallint'  => 'int',
    'integer'   => 'int',
    'bigint'    => 'bigint',
    'decimal'   => 'decimal',
    'float'     => 'float',
    'string'    => 'varchar',
    'text'      => 'varchar',
    'guid'      => 'uuid',
    'binary'    => 'blob',
    'blob'      => 'blob',
    'boolean'   => 'boolean',
    'date'      => 'timestamp',
    'datetime'  => 'timestamp',
    'datetimez' => 'timestamp',
    'time'      => 'timestamp',
  ];

  private static $_cqlPackTypes = ['int', 'timestamp', 'counter'];

  protected static function _getCqlField($map)
  {
    return self::_escapeIdentifier($map['columnName']) . ' '
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

  protected static function _mustPack($map)
  {
    return array_search(
      static::_getCqlFieldType($map),
      self::$_cqlPackTypes
    ) !== false;
  }

  protected static function _pack($value)
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

  protected static function _unpack($data)
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
