<?php
use Packaged\Mappers\DoctrineMapper;

/**
 * @author  Richard.Gooding
 */
class NonTimestampMapper extends DoctrineMapper
{
  public $someData;
  public $id;

  public static function getAutoTimestamp()
  {
    return false;
  }
}
