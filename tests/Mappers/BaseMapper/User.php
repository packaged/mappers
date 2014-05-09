<?php

/**
 * @Entity @Table(name="users")
 **/
class User extends \Packaged\Mappers\BaseMapper
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
}
