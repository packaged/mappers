<?php
use Respect\Validation\Validator;

class CassPerson extends \Packaged\Mappers\CassandraMapper
{
  public $id;
  public $name;
  public $description;
  public $testField;

  protected function _configure()
  {
    $this->_addValidator('name', Validator::notEmpty()->length(2, 32));
    $this->_addValidator('testField', Validator::string()->endsWith('test'));
  }
}
