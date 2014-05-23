<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 20/05/2014
 * Time: 16:31
 */

namespace Packaged\Mappers;

use cassandra\InvalidRequestException;
use Packaged\Mappers\Exceptions\InvalidLoadException;
use Packaged\Mappers\Exceptions\MapperException;

abstract class CassandraMapper extends BaseMapper
{

  public static function getServiceName()
  {
    return 'cassdb';
  }

  /**
   * Load a mapper with a specific id
   *
   * @param mixed $id
   *
   * @return IMapper|static
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
      $return = new static();
      if($data)
      {
        $data[$return->keyField()] = $id;
        $return->hydrate($data);
        $return->setExists(true);
      }
      return $return;
    }
  }

  public static function getData($id)
  {
    $result = self::execute(
      'SELECT * FROM ' . static::getTableName() . ' WHERE KEY = ?',
      [$id]
    );
    $data   = [];
    foreach($result as $row)
    {
      if(isset($row['key'])
        && isset($row['column1'])
        && isset($row['value'])
      )
      { // for dynamic tables, column# => value
        $data[$row['column1']] = $row['value'];
      }
      else
      {
        $data = $row;
      }
    }
    return $data;
  }

  /**
   * @param $query
   * @param $parameters
   *
   * @return \PDOStatement
   * @throws \Exception
   */
  public static function execute($query, array $parameters = [])
  {
    try
    {
      $conn   = static::getConnection();
      $stmt   = $conn->prepare($query);
      $return = $stmt->execute($parameters);
    }
    catch(InvalidRequestException $e)
    {
      throw new \Exception($e->why);
    }
    return $return;
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

    $changes = call_user_func('get_object_vars', $this);
    unset($changes[$this->keyField()]);

    /* Column Family Format */
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
    $this->setExists(true);
    return $return;
    /* CQL3 Format */
    /*$query = sprintf(
      "INSERT INTO %s (%s) VALUES(%s)",
      static::getTableName(),
      implode(', ', array_keys($changes)),
      implode(',', array_fill(0, count($changes), '?'))
    );
    $stmt = $conn->prepare($query);
    return $stmt->execute($changes);*/
  }

  public function saveAsNew($newKey = null)
  {
    $return = new static();
    foreach($this as $k => $v)
    {
      $return->$k = $v;
    }
    $keyField          = $this->keyField();
    $return->$keyField = $newKey;
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
    self::execute(
      'DELETE FROM ' . static::getTableName()
      . ' WHERE KEY = ?',
      [$this->id()]
    );
    $this->setExists(false);
  }

  public function id()
  {
    $keyField = $this->keyField();
    return $this->$keyField;
  }

  public function increment($field, $count)
  {
    throw new MapperException('increment not supported');
    /*self::execute(
      'UPDATE ' . static::getTableName() . ' SET "'
      . $field . '" = "' . $field . '" + ? WHERE KEY = ?',
      [$count, $this->id()]
    );
    $this->$field += $count;*/
  }

  public function decrement($field, $count)
  {
    throw new MapperException('decrement not supported');
    /*self::execute(
      'UPDATE ' . static::getTableName() . ' SET "'
      . $field . '" = "' . $field . '" - ? WHERE KEY = ?'
      ,
      [$count, $this->id()]
    );
    $this->$field -= $count;*/
  }

  public static function getTableName()
  {
    return strtolower(basename(get_called_class()));
  }

  abstract public function keyField();

  public function setId($value)
  {
    $keyField        = $this->keyField();
    $this->$keyField = $value;
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
}
