<?php
/**
 * @author  Richard.Gooding
 */
use Packaged\Mappers\DoctrineMapper;

/**
 * @Entity
 */
class PartAnnotatedMapper extends DoctrineMapper
{
  public $id;
  public $username;
  /**
   * @Column(type="string")
   */
  public $displayName;
  public $createdOn;
  /**
   * @Column(type="datetime")
   */
  public $lastLogin;
}
