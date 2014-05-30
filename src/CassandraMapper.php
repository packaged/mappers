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
  protected static $_queryRetries = 3;
  protected static $_autoTimeFormat = self::AUTOTIME_FORMAT_TIMESTAMP;

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
      $md      = self::_getMetadata();
      $columns = [];
      foreach($md->fieldMappings as $map)
      {
        $columns[] = static::_getCqlField($map);
      }
      $query = 'CREATE TABLE "' . $table . '" ('
        . implode(',', $columns)
        . ', PRIMARY KEY (' . implode(',', $md->identifier) . '))';
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
    return '"' . $map['columnName'] . '" ' . static::_getCqlFieldType($map);
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

  protected static function _mustPack($map)
  {
    return array_search(
      static::_getCqlFieldType($map),
      self::$_cqlPackTypes
    ) !== false;
  }

  protected static function _pack($value)
  {
    $highMap = 0xffffffff00000000;
    $lowMap  = 0x00000000ffffffff;
    $higher  = ($value & $highMap) >> 32;
    $lower   = $value & $lowMap;
    $packed  = pack('NN', $higher, $lower);
    return $packed;
  }

  protected static function _unpack($packed)
  {
    list($higher, $lower) = array_values(unpack('N2', $packed));
    return $higher << 32 | $lower;
  }
}
