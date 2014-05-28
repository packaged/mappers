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

  public function hydrate(array $values)
  {
    foreach(static::_getMetadata()->fieldMappings as $map)
    {
      if(!isset($map['id']) || !$map['id'])
      {
        $values[$map['columnName']] = self::unpack($values[$map['columnName']]);
      }
    }
    return parent::hydrate($values);
  }

  public function increment($field, $count)
  {
    $column = static::_getMetadata()->columnNames[$field];
    $keys   = [];
    foreach(self::_getKeys() as $k)
    {
      $keys[] = '"' . $k . '" = ?';
    }
    $values = [self::pack($count)] + $this->id();
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
    $values = [self::pack($count)] + $this->id();
    self::execute(
      'UPDATE ' . static::getTableName() . ' SET "'
      . $column . '" = "' . $column . '" - ? WHERE ' . implode(' AND ', $keys),
      $values
    );
    $this->$field -= $count;
  }

  private static function pack($value)
  {
    $highMap = 0xffffffff00000000;
    $lowMap  = 0x00000000ffffffff;
    $higher  = ($value & $highMap) >> 32;
    $lower   = $value & $lowMap;
    $packed  = pack('NN', $higher, $lower);
    return $packed;
  }

  private static function unpack($packed)
  {
    list($higher, $lower) = array_values(unpack('N2', $packed));
    return $higher << 32 | $lower;
  }
}
