<?php
/**
 * Created by PhpStorm.
 * User: Oridan
 * Date: 26/05/2014
 * Time: 10:56
 */

namespace Packaged\Mappers;

abstract class CounterCassandraMapper extends CassandraMapper
{
  public static function useAutoTimestamp()
  {
    return false;
  }

  protected static function _getCqlFieldType($map)
  {
    $keyCols = self::_getKeyColumns();

    if((isset($map['id']) && $map['id'])
      || in_array($map['columnName'], $keyCols)
    )
    {
      return parent::_getCqlFieldType($map);
    }
    return self::CASSANDRA_TYPE_COUNTER;
  }

  public function increment($field, $count)
  {
    $column = static::_getFieldMap()[$field];
    $keys   = [];
    foreach(self::_getKeyColumns() as $k)
    {
      $keys[] = '"' . $k . '" = ?';
    }
    $values = array_merge(
      [self::_pack($count, self::CASSANDRA_TYPE_COUNTER)],
      (array)$this->id()
    );
    self::execute(
      'UPDATE ' . static::getTableName() . ' SET "'
      . $column . '" = "' . $column . '" + ? WHERE ' . implode(' AND ', $keys),
      $values
    );
    $this->$field += $count;
  }

  public function decrement($field, $count)
  {
    $column = static::_getFieldMap()[$field];
    $keys   = [];
    foreach(self::_getKeyColumns() as $k)
    {
      $keys[] = '"' . $k . '" = ?';
    }
    $values = array_merge(
      [self::_pack($count, self::CASSANDRA_TYPE_COUNTER)],
      (array)$this->id()
    );
    self::execute(
      'UPDATE ' . static::getTableName() . ' SET "'
      . $column . '" = "' . $column . '" - ? WHERE ' . implode(' AND ', $keys),
      $values
    );
    $this->$field -= $count;
  }
}
