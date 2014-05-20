<?php
use Packaged\Mappers\BaseMapper;

/**
 * @author  Richard.Gooding
 */
class NonTimestampMapper extends BaseMapper
{
  public $someData;
  public $id;

  public static function getAutoTimestamp()
  {
    return false;
  }
}
