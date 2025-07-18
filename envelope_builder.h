#pragma once

#include <string>
#include <filesystem>
#include <vector>
#include <unordered_map>
#include <set>
#include <map>

#include "sqlite3.h"

namespace fs = std::filesystem;

namespace Builder
{
    /**
     * @class EnvelopeBuilder
     * @brief A utility to process a set of SQLite databases, verify their consistency,
     * and create a final "enveloped" database containing maximum values.
     * It can generate two types of output databases: one with standard enveloping
     * and another with special handling for shell elements (elemType=2), where
     * shear reinforcement values (Asw1, Asw2) are summed before enveloping.
     */
    class EnvelopeBuilder
    {
    public:
        EnvelopeBuilder();
        
        /**
         * @brief Runs the main build process.
         */
        void Run();

    private:
        // Internal configuration constants
        struct Config
        {
            const std::string ELEMENTS_TABLE_NAME = "Elements";
            const std::string ELEMENT_ID_COLUMN = "elemId";
            const std::string SET_N_COLUMN = "setN";
            const std::string ELEM_TYPE_COLUMN = "elemType";
            const std::string OUTPUT_DB_FILENAME = "Envelope.db";
            const std::string OUTPUT_DB_SUMMED_FILENAME = "Envelope_Summed.db";
            const std::string ENVELOPED_TABLE_NAME = "Enveloped Reinforcement";
        };

        // Type aliases for clarity
        using ElementProperties = std::unordered_map<std::string, std::string>;
        using VerifiedElementsMap = std::unordered_map<long long, ElementProperties>;
        using EnvelopedDataMap = std::unordered_map<long long, std::unordered_map<std::string, double>>;

        Config config_;
        VerifiedElementsMap verifiedElements_; // Stores properties of unique elements
        EnvelopedDataMap envelopedData_;       // Stores the enveloped (maximum) values

        // --- Main Build Stages ---

        /**
         * @brief PASS 1: Iterates through all .db files to collect and verify element properties.
         * @param targetPath The directory containing the source .db files.
         * @return True if verification is successful, false otherwise.
         */
        bool CollectAndVerifyElements(const fs::path& targetPath);

        /**
         * @brief PASS 2: Iterates through all .db files to find the maximum (enveloped) values for all numeric columns.
         * For shell elements, it also calculates and envelops the sum of shear reinforcement.
         * @param targetPath The directory containing the source .db files.
         */
        void EnvelopeDataInMemory(const fs::path& targetPath);

        /**
         * @brief PASS 3: Assembles the final database from the in-memory data.
         * @param targetPath The directory where the final database will be saved.
         * @param createSummedVersion If true, creates the database with summed shear reinforcement for shells.
         */
        void AssembleFinalDatabase(const fs::path& targetPath, bool createSummedVersion);

        // --- Helper Methods ---

        /**
         * @brief Prompts the user to enter the path to the directory with .db files.
         * @return A valid filesystem path, or an empty path if input is invalid.
         */
        fs::path GetTargetPathFromUser();

        /**
         * @brief Logs a SQLite error message to stderr.
         * @param message A custom message to prepend to the error.
         * @param dbHandle The SQLite database handle.
         */
        void LogSqliteError(const std::string& message, sqlite3* dbHandle);

        /**
         * @brief Retrieves a list of all table names from a database.
         * @param dbHandle The SQLite database handle.
         * @return A vector of table names.
         */
        std::vector<std::string> GetTableNames(sqlite3* dbHandle);

        /**
         * @brief Collects all unique column headers from the enveloped data, excluding internal fields.
         * @return A set of column names.
         */
        std::set<std::string> CollectAllEnvelopedColumns();
    };
}

