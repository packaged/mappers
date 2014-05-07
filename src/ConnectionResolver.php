<?php
namespace Packaged\Mappers;

class ConnectionResolver implements IConnectionResolver
{
  protected $_connections;

  public function getConnection($name)
  {
    if(isset($this->_connections[$name]))
    {
      return $this->_connections[$name];
    }
  }

  public function addConnection($name, $connection)
  {
    $this->_connections[$name] = $connection;
  }
}
