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
      $data   = self::getData($id);
      $mapper = new static();
      if($data)
      {
        $mapper->hydrate($data);
        $mapper->setExists(true);
      }
      return $mapper;
    }
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
    $this->validate();

    $changes  = [];
    $mappings = static::_getColumnMap();
    foreach($mappings as $column => $field)
    {
      $changes[$column] = $this->$field;
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
      /*if(static::UseWideRows())
      {
        $conn->prepare(
          'CREATE TABLE IF NOT EXISTS "Cubex"."cass_users" (key blob, column1 ascii, value blob, PRIMARY KEY (key, column1)) WITH COMPACT STORAGE;'
        );
        $conn->execute([]);
      }*/
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

  private static $cqlTypes = [
    'ascii'     => 'string',
    'bigint'    => 'bigint',
    'blob'      => 'blob',
    'boolean'   => 'boolean',
    'decimal'   => 'float',
    'double'    => 'float',
    'float'     => 'float',
    'inet'      => 'string',
    'int'       => 'integer',
    'set'       => 'simple_array',
    'text'      => 'string',
    'timestamp' => 'time',
    'uuid'      => 'uuid',
    'varchar'   => 'string',
    'varint'    => 'integer',
    'counter'   => 'integer',
  ];

  protected static function _getCqlField($map)
  {
    if(isset(self::$cqlTypes[$map['type']]))
    {
      $type = self::$cqlTypes[$map['type']];
    }
    else
    {
      $type = array_search($map['type'], self::$cqlTypes);
    }
    return '"' . $map['columnName'] . '" ' . $type;
  }
}
