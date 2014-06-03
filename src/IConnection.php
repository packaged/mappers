<?php
/**
 * @author  Richard.Gooding
 */

namespace Packaged\Mappers;

interface IConnection
{
  /**
   * @param string $query
   *
   * @return ICQLPreparedStatement
   */
  public function prepare($query);

  /**
   * @param ICQLPreparedStatement $statement
   * @param array|null            $params
   *
   * @return mixed
   */
  public function execute(ICQLPreparedStatement $statement, array $params = null);
}
