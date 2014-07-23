<?php
/**
 * Created by PhpStorm.
 * User: Oridan
 * Date: 20/07/2014
 * Time: 15:07
 */

namespace Packaged\Mappers;

class PDOConnection extends \PDO implements IConnection
{
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

  public function prepare($statement, $options = null)
  {
    return parent::prepare($statement, $options);
  }
}