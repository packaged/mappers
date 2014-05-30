<?php
use Packaged\Mappers\CassandraMapper;

/**
 * @Entity
 * @Table(name="keyed_users",indexes={@Index(name="primary_key",columns={"email","username"})})
 */
class KeyedUser extends CassandraMapper
{
  /**
   * @Id
   * @Column(type="int")
   */
  public $brandId;
  /**
   * @Id
   * @Column(type="int")
   */
  public $userId;
  public $email;
  public $username;
  public $password;
  public $displayName;
}
