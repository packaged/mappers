<?php
namespace Packaged\Mappers;

interface IConnectionResolver
{
  /**
   * @param $name
   *
   * @return IConnection
   */
  public function getConnection($name);
}
