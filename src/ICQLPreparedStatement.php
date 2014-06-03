<?php
/**
 * @author  Richard.Gooding
 */

namespace Packaged\Mappers;

interface ICQLPreparedStatement
{
  public function execute(array $params = []);
}
