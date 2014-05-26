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
  public static function createTable()
  {
    if(static::UseCompactStorage())
    {
      static::execute('CREATE TABLE IF NOT EXISTS "Cubex"."cass_users" (key blob, column1 ascii, value counter, PRIMARY KEY (key, column1)) WITH COMPACT STORAGE;');
    }
    else
    {
    }
  }

  public function increment($field, $count)
  {
    self::execute(
      'UPDATE ' . static::getTableName() . ' SET "'
      . $field . '" = "' . $field . '" + ? WHERE KEY = ?',
      [$count, $this->id()]
    );
    $this->$field += $count;
  }

  public function decrement($field, $count)
  {
    self::execute(
      'UPDATE ' . static::getTableName() . ' SET "'
      . $field . '" = "' . $field . '" - ? WHERE KEY = ?'
      ,
      [$count, $this->id()]
    );
    $this->$field -= $count;
  }
}
