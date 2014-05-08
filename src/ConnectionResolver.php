<?php
namespace Packaged\Mappers;

use Packaged\Mappers\Exceptions\MapperException;

class ConnectionResolver implements IConnectionResolver
{
  protected $_connections;

  public function getConnection($name)
  {
    if(isset($this->_connections[$name]))
    {
      return $this->_connections[$name];
    }
    throw new MapperException('Connection (' . $name . ') not found');
  }

  public function addConnection($name, $connection)
  {
    $this->_connections[$name] = $connection;
  }
}
