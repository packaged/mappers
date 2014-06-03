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
   * @return IPreparedStatement
   */
  public function prepare($query);

  /**
   * @param IPreparedStatement $statement
   * @param array|null            $params
   *
   * @return mixed
   */
  public function execute(IPreparedStatement $statement, array $params = null);
}
