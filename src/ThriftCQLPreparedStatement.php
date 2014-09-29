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
  private $_query;
  private $_compression;
  private $_host;

  public function __construct(
    ThriftConnection $connection, CqlPreparedResult $rawStatement,
    $query, $compression
  )
  {
    $this->_connection   = $connection;
    $this->_host         = $connection->socket()->getHost();
    $this->_rawStatement = $rawStatement;
    $this->_query        = $query;
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

  public function getQuery()
  {
    return $this->_query;
  }

  public function getCompression()
  {
    return $this->_compression;
  }

  public function isHost($host)
  {
    return $this->_host == $host;
  }
}
