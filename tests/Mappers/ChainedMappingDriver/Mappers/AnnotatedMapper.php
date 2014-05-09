<?php
/**
 * @author  Richard.Gooding
 */

/**
 * @Entity @Table(name="test_table")
 */
class AnnotatedMapper
{
  /**
   * @Id @Column(type="integer") @GeneratedValue
   */
  public $id;
  /**
   * @Column(type="string")
   */
  public $username;
  /**
   * @Column(type="string")
   */
  public $displayName;
  /**
   * @Column(type="date")
   */
  public $createdOn;
  /**
   * @Column(type="datetime")
   */
  public $lastLogin;
}
