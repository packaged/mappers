<?php
/**
 * @author  Richard.Gooding
 */
use Packaged\Mappers\BaseMapper;

/**
 * @Entity
 */
class PartAnnotatedMapper extends BaseMapper
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
