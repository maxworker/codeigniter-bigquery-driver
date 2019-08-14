<?php
/**
 * CodeIgniter
 *
 * An open source application development framework for PHP
 *
 * This content is released under the MIT License (MIT)
 *
 */
defined('BASEPATH') or exit('No direct script access allowed');

/**
 * BigQuery Database Adapter Class
 *
 * Note: _DB is an extender class that the app controller
 * creates dynamically based on whether the query builder
 * class is being used or not.
 *
 * @package       CodeIgniter
 * @subpackage    Drivers
 * @category      Database
 * @author        Maxim Butenko based EllisLab Dev Team
 * @link          https://codeigniter.com/user_guide/database/
 */
class CI_DB_bigquery_driver extends CI_DB {

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'bigquery';

    /**
     * BigQuery Options
     *
     * @var    array
     */
    public $options = array();

    /**
     * BigQuery Intermediate Helper
     *
     * @var    Object
     */
    protected $bq;

    /**
     * BigQuery Connection Options
     *
     * @var    array
     */
    //private $connectionOptions;

    /**
     * BigQuery Current Job
     *
     * @var    object
     */
    private $job;


    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @param    array    $params
     * @return    void
     */
    public function __construct($params)
    {
        parent::__construct($params);
    }

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param    bool    $persistent
     * @return    object
     */
    public function db_connect($persistent = FALSE)
    {
        try
        {
            $CI =& get_instance();
            $CI->load->library('BigQuery');
            $connectionOptions = ['BQ_KEY_FILE' => $this->username, 'BQ_PROJECT_ID' => $this->hostname, 'BQ_DATASET' => $this->database];
            $this->bq = new BigQuery($connectionOptions);
            return $this->bq;
        }
        catch (Exception $e)
        {
            return FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Database version number
     *
     * @return    string
     */
    public function version()
    {
        return parent::version();
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @param    string    $sql    SQL query
     * @return    mixed
     */
    protected function _execute($sql)
    {
        return $this->conn_id->query($sql);
    }

    // --------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @return    bool
     */
    protected function _trans_begin()
    {
        return false;
    }

    // --------------------------------------------------------------------

    /**
     * Commit Transaction
     *
     * @return    bool
     */
    protected function _trans_commit()
    {
        return false;
    }

    // --------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @return    bool
     */
    protected function _trans_rollback()
    {
        return false;
    }

    // --------------------------------------------------------------------

    /**
     * Affected Rows
     *
     * @return    int
     */
    public function affected_rows()
    {
        return $this->conn_id->getQueryTotalRows();
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @param    string    $name
     * @return    int
     */
    public function insert_id($name = NULL)
    {
        return $this->conn_id->getJobId();
        return false;
    }

    // --------------------------------------------------------------------

    /**
     * Field data query
     *
     * Generates a platform-specific query so that the column data can be retrieved
     *
     * @param    string    $table
     * @return    string
     */
    protected function _field_data($table)
    {
        return $this->bq->getQuerySchema();
    }

    // --------------------------------------------------------------------

    /**
     * Error
     *
     * Returns an array containing code and message of the last
     * database error that has occured.
     *
     * @return    array
     */
    public function error()
    {
        $errors = $bq->getQueryErrors();

        $error = array('code' => '00000', 'message' => '');
        if (count($errors) > 0)
        {
            $message = "";
            foreach ($errors as $value) {
                $message .= $value["message"]." \n";
            }
            $error['message'] = $message;
        }

        return $error;
    }

    // --------------------------------------------------------------------

    /**
     * Update_Batch statement
     *
     * Generates a platform-specific batch update string from the supplied data
     *
     * @param    string    $table    Table name
     * @param    array    $values    Update data
     * @param    string    $index    WHERE key
     * @return    string
     */
    protected function _update_batch($table, $values, $index)
    {
        return '';
    }

    // --------------------------------------------------------------------

    /**
     * Truncate statement
     *
     * Generates a platform-specific truncate string from the supplied data
     *
     * If the database does not support the TRUNCATE statement,
     * then this method maps to 'DELETE FROM table'
     *
     * @param    string    $table
     * @return    string
     */
    protected function _truncate($table)
    {
        return '';
    }
}
