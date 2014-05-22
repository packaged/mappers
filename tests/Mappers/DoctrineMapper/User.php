<?php

/**
 * @Entity @Table(name="users")
 **/
class User extends \Packaged\Mappers\DoctrineMapper
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
   * @Column(type="integer")
   */
  public $countField = 0;
}
