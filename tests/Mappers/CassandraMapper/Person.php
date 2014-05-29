<?php
use Respect\Validation\Validator;

/**
 * Class CassPerson
 * @Entity
 */
class CassPerson extends \Packaged\Mappers\CassandraMapper
{
  /**
   * @Id @Column(type="string")
   */
  public $hash;
  public $name;
  public $description;
  public $testField;

  protected function _configure()
  {
    $this->_addValidator('name', Validator::notEmpty()->length(2, 32));
    $this->_addValidator('testField', Validator::string()->endsWith('test'));
  }
}
