<?php
use Packaged\Mappers\BaseMapper;

/**
 * @author  Richard.Gooding
 */

class NotNamespacedMapper extends BaseMapper
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
