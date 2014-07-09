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
  protected static function _getCqlFieldType($map)
  {
    if(isset($map['id']) && $map['id'])
    {
      return parent::_getCqlFieldType($map);
    }
    return 'counter';
  }

  public function increment($field, $count)
  {
    $column = static::_getFieldMap()[$field];
    $keys   = [];
    foreach(self::_getKeyColumns() as $k)
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
    $column = static::_getFieldMap()[$field];
    $keys   = [];
    foreach(self::_getKeyColumns() as $k)
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
