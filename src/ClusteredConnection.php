<?php
/**
 * Created by PhpStorm.
 * User: Oridan
 * Date: 20/07/2014
 * Time: 14:35
 */

namespace Packaged\Mappers;

class ClusteredConnection implements IConnection
{
  const MODE_READ = 'r';
  const MODE_WRITE = 'w';

  /**
   * @var \PDO[]
   */
  protected $_masters = [];

  /**
   * @var \PDO[]
   */
  protected $_slaves = [];

  protected $_sticky;
  protected $_stickyMaster;
  protected $_stickySlave;

  public function addMaster(IConnection $connection)
  {
    $this->_masters[] = $connection;
  }

  public function addSlave(IConnection $connection)
  {
    $this->_slaves[] = $connection;
  }

  public function __construct($sticky = false)
  {
    $this->_sticky = $sticky;
  }

  /**
   * @param string  $mode
   * @param boolean $sticky
   *
   * @returns IConnection
   *
   * @throws \Exception
   */
  public function getConnection($mode = self::MODE_WRITE, $sticky = null)
  {
    if($sticky === null)
    {
      $sticky = $this->_sticky;
    }

    $connection = null;

    if($mode === self::MODE_READ && $this->_slaves)
    {
      // select slave
      if(!$this->_stickySlave)
      {
        shuffle($this->_slaves);
        $this->_stickySlave = head($this->_slaves);
      }
      if($sticky)
      {
        $connection = $this->_stickySlave;
      }
      else
      {
        shuffle($this->_slaves);
        $connection = head($this->_slaves);
      }
    }
    else
    {
      // default to master for safety
      if(!$this->_stickyMaster)
      {
        shuffle($this->_masters);
        $this->_stickyMaster = head($this->_masters);
      }
      if($sticky)
      {
        $connection = $this->_stickyMaster;
      }
      else
      {
        shuffle($this->_masters);
        $connection = head($this->_masters);
      }
    }

    if($connection)
    {
      throw new \Exception('No server available in mode (' . $mode . ')');
    }

    return $connection;
  }

  /**
   * @param string $query
   * @param string $mode
   *
   * @return IPreparedStatement
   */
  public function prepare($query, $mode = self::MODE_WRITE)
  {
    $connection = $this->getConnection($mode);
    return $connection->prepare($query);
  }

  /**
   * @param IPreparedStatement $statement
   * @param array|null         $params
   *
   * @return mixed
   */
  public function execute(IPreparedStatement $statement, array $params = null)
  {
    return $statement->execute($params);
  }
}