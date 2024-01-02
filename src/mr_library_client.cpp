#include <iostream>
#include <vector>
#include <string>
#include <cctype>

struct KeyValue {
    std::string key;
    std::string value;
};

/**
 * @brief Split a string into words
 * 
 * @param text Input text content
 * @return std::vector<std::string> List of words after splitting
 */
std::vector<std::string> split_str_in_words(const std::string& text) {
    std::vector<std::string> words;
    std::string word;
    
    for (char ch : text) {
        if (std::isalpha(ch)) {
            word += ch;
        } else {
            if (!word.empty()) { // a word is formed.
                words.push_back(word);
                word.clear();
            }
        }
    }

    if (!word.empty()) {
        words.push_back(word);
    }
    return words;
}

/**
 * @brief Map function: Split the text into words and store them with a value of "1"
 * 
 * @param kv KeyValue containing the text to process
 * @return std::vector<KeyValue> List of key-value pairs
 */
std::vector<KeyValue> map_func(KeyValue kv) {
    std::vector<KeyValue> result;
    std::vector<std::string> words = split_str_in_words(kv.key);

    for (const auto& word : words) {
        KeyValue tmp;
        tmp.key = word;
        tmp.value = "1";
        result.emplace_back(tmp);
    }

    return result;
}


/**
 * @brief Reduce function: Output the length of each value in the input key-value pairs
 * 
 * @param kvs List of key-value pairs
 * @param reduce_task_id Redundant parameter (left for compatibility)
 * @return std::vector<std::string> List of reduced values
 */
std::vector<std::string> reduce_func(std::vector<KeyValue> kvs, int reduce_task_id) {
    std::vector<std::string> result;

    for (const auto& kv : kvs) {
        result.push_back(std::to_string(kv.value.size()));
    }

    return result;
}
