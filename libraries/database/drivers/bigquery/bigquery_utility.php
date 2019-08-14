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
 * BigQuery Utility Class Plug
 *
 * @package       CodeIgniter
 * @subpackage    Drivers
 * @category      Database
 * @author        EllisLab Dev Team
 * @link          https://codeigniter.com/database/
 */
class CI_DB_bigquery_utility extends CI_DB_utility {

    /**
     * Export
     *
     * @param    array    $params    Preferences
     * @return    mixed
     */
    protected function _backup($params = array())
    {
        // Currently unsupported
        return $this->db->display_error('db_unsupported_feature');
    }
}
