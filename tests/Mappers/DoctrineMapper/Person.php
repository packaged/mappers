<?php
use Respect\Validation\Validator;

/**
 * @author  Richard.Gooding
 */

/**
 * @Entity @Table(name="people")
 */
class Person extends \Packaged\Mappers\DoctrineMapper
{
  /**
   * @Id @Column(type="integer") @GeneratedValue
   **/
  public $id;

  /**
   * @Column(type="string")
   **/
  public $name;

  /**
   * @Column(type="string")
   **/
  public $description;

  /**
   * @Column(type="string")
   */
  public $testField;

  protected function _configure()
  {
    $this->_addValidator('name', Validator::notEmpty()->length(2, 32));
    $this->_addValidator('testField', Validator::string()->endsWith('test'));
  }
}
