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
 * BigQuery Result Class
 *
 * This class extends the parent result class: CI_DB_result
 *
 * @package       CodeIgniter
 * @subpackage    Drivers
 * @category      Database
 * @author        Maxim Butenko based EllisLab Dev Team
 * @link          https://codeigniter.com/user_guide/database/
 */
class CI_DB_bigquery_result extends CI_DB_result {

    private $currentRowNumber = 0;

    /**
     * Number of rows in the result set
     *
     * @return    int
     */
    public function num_rows()
    {
        $this->num_rows = $this->result_id->num_rows();
        return $this->num_rows;
    }

    // --------------------------------------------------------------------

    /**
     * Number of fields in the result set
     *
     * @return    int
     */
    public function num_fields()
    {
        return $this->result_id->num_fields();
    }

    // --------------------------------------------------------------------

    /**
     * Fetch Field Names
     *
     * Generates an array of column names
     *
     * @return    array
     */
    public function list_fields()
    {
        return $this->result_id->list_fields();
    }

    // --------------------------------------------------------------------

    /**
     * Field data
     *
     * Generates an array of objects containing field meta-data
     *
     * @return    array
     */
    public function field_data()
    {
        return $this->result_id->field_data();
    }

    // --------------------------------------------------------------------

    /**
     * Free the result
     *
     * @return    void
     */
    public function free_result()
    {
        if (is_object($this->result_id))
        {
            $this->result_id = false;
        }
    }

    // --------------------------------------------------------------------


    /**
     * Data Seek
     *
     * Moves the internal pointer to the desired offset. We call
     * this internally before fetching results to make sure the
     * result set starts at zero.
     *
     * @param    int    $n
     * @return    bool
     */
    public function data_seek($n = 0)
    {
        $this->currentRowNumber = $n;
        return true;
    }

    // --------------------------------------------------------------------

    /**
     * Result - associative array
     *
     * Returns the result set as an array
     *
     * @return    array
     */
    protected function _fetch_assoc()
    {
        $results = $this->result_id->result_data();
        if (get_class($results) == "Google\Cloud\Core\Iterator\ItemIterator") {
            $data = $results->current();
            $results->next();
            return $data;
        } elseif (get_class($results) == "Google\Cloud\Core\Iterator\PageIterator") {
            if ($this->currentRowNumber < count($results->current())) {
                $data = $results->current()[$this->currentRowNumber];
                $this->currentRowNumber = $this->currentRowNumber + 1;
                return $data;
            } else {
                return null;
            }
        }
        return null;
    }

    // --------------------------------------------------------------------

    /**
     * Result - object
     *
     * Returns the result set as an object
     *
     * @param    string    $class_name
     * @return    object
     */
    protected function _fetch_object($class_name = 'stdClass')
    {
        return (object)$this->_fetch_assoc();
    }
}
