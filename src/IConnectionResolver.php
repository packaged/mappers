<?php
namespace Packaged\Mappers;

interface IConnectionResolver
{
  public function getConnection($name);
}
