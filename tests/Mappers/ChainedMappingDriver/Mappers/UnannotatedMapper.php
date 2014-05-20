<?php
use Packaged\Mappers\DoctrineMapper;

/**
 * @author  Richard.Gooding
 */
class UnannotatedMapper extends DoctrineMapper
{
  public $id;
  public $age;
  public $notes;

  public $price;
  public $tax;
  public $amount;
  public $cost;
  public $total;

  public $createdAt;
  public $updateTime;

  public $lastViewedOn;
  public $expiryDate;

  public $comment;
}
