<?php
/**
 * CodeIgniter
 *
 * An open source application development framework for PHP
 *
 * This content is released under the MIT License (MIT)
 *
 */
defined('BASEPATH') OR exit('No direct script access allowed');

/**
 * BigQuery Forge Class Plug
 *
 * @package       CodeIgniter
 * @subpackage    Drivers
 * @category      Database
 * @author        EllisLab Dev Team
 * @link          https://codeigniter.com/database/
 */
class CI_DB_bigquery_forge extends CI_DB_forge {

    /**
     * CREATE TABLE IF statement
     *
     * @var    string
     */
    protected $_create_table_if    = FALSE;

    /**
     * DROP TABLE IF statement
     *
     * @var    string
     */
    protected $_drop_table_if    = FALSE;

}
