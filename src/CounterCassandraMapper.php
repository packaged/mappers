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
  protected static function _getCqlField($map)
  {
    if(isset($map['id']) && $map['id'])
    {
      return parent::_getCqlField($map);
    }
    return '"' . $map['columnName'] . '" counter';
  }

  public function increment($field, $count)
  {
    $column = static::_getMetadata()->columnNames[$field];
    $keys   = [];
    foreach(self::_getKeys() as $k)
    {
      $keys[] = '"' . $k . '" = ?';
    }
    $values = [self::_pack($count)] + $this->id();
    self::execute(
      'UPDATE ' . static::getTableName() . ' SET "'
      . $column . '" = "' . $column . '" + ? WHERE ' . implode(' AND ', $keys),
      $values
    );
    $this->$field += $count;
  }

  public function decrement($field, $count)
  {
    $column = static::_getMetadata()->columnNames[$field];
    $keys   = [];
    foreach(self::_getKeys() as $k)
    {
      $keys[] = '"' . $k . '" = ?';
    }
    $values = [self::_pack($count)] + $this->id();
    self::execute(
      'UPDATE ' . static::getTableName() . ' SET "'
      . $column . '" = "' . $column . '" - ? WHERE ' . implode(' AND ', $keys),
      $values
    );
    $this->$field -= $count;
  }
}
