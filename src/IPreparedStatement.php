<?php
/**
 * @author  Richard.Gooding
 */

namespace Packaged\Mappers;

interface IPreparedStatement
{
  public function execute(array $params = []);
}
