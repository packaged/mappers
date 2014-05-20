<?php

/**
 * @author  Richard.Gooding
 */
class DifferentFieldsMapper extends \Packaged\Mappers\DoctrineMapper
{
  public $id;
  public $someData;

  public static function getAutoTimestamp()
  {
    return true;
  }

  public static function getCreatedAtColumn()
  {
    return 'other_created_at';
  }

  public static function getUpdatedAtColumn()
  {
    return 'other_updated_at';
  }
}
