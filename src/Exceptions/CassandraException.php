<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 28/05/2014
 * Time: 18:24
 */

namespace Packaged\Mappers\Exceptions;

class CassandraException extends \Exception
{
  public function __construct($msg = "", $code = 0, \Exception $previous = null)
  {
    if($previous !== null)
    {
      $prevMsg = null;
      if(isset($previous->why))
      {
        $prevMsg = $previous->why;
      }
      else
      {
        $prevMsg = $previous->getMessage();
      }

      if((!empty($prevMsg)) && ($prevMsg != $msg))
      {
        $msg = $prevMsg . "\n" . $msg;
      }
    }
    parent::__construct($msg, $code, $previous);
  }
}