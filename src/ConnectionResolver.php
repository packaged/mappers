<?php
namespace Packaged\Mappers;

use Packaged\Mappers\Exceptions\MapperException;

class ConnectionResolver implements IConnectionResolver
{
  protected $_connections;

  public function getConnection($name)
  {
    if(!isset($this->_connections[$name]))
    {
      throw new MapperException('Connection (' . $name . ') not found');
    }

    if(is_callable($this->_connections[$name]))
    {
      $this->_connections[$name] = $this->_connections[$name]();
    }

    return $this->_connections[$name];
  }

  public function addConnection($name, $connection)
  {
    $this->_connections[$name] = $connection;
  }
}
