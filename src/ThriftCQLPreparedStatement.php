<?php
/**
 * @author  Richard.Gooding
 */

namespace Packaged\Mappers;

use cassandra\CqlPreparedResult;

class ThriftCQLPreparedStatement implements IPreparedStatement
{
  private $_rawStatement;
  private $_connection;

  public function __construct(
    ThriftConnection $connection, CqlPreparedResult $rawStatement
  )
  {
    $this->_connection = $connection;
    $this->_rawStatement = $rawStatement;
  }

  public function execute(array $params = [])
  {
    return $this->_connection->execute($this, $params);
  }

  public function getRawStatement()
  {
    return $this->_rawStatement;
  }

  public function getQueryId()
  {
    return $this->_rawStatement->itemId;
  }
}
