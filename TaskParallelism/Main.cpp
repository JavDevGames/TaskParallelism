#include <iostream>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <sstream>
#include <filesystem>
#include "concurrentqueue.h"
#include <cstdio>

using namespace std;
namespace fs = std::filesystem;

void ParseTestFiles();
void CreateTestFiles();
void ParseTestFilesConcurrent();

int main()
{
    CreateTestFiles();
 
    //Single threaded
    ParseTestFiles();

    //Single threaded
    ParseTestFilesConcurrent();
}

void CreateTestFiles()
{
    int numFiles = 1000; // Number of files to generate
    int wordsPerFile = 1000; // Number of words per file
    string directory = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles\\"; // Directory to save files in

    for (int i = 1; i <= numFiles; i++) {
        stringstream fileName;
        fileName << directory << "file" << i << ".txt"; // Generate file name
        ofstream outFile(fileName.str()); // Open output file
        if (outFile.is_open()) {
            for (int j = 1; j <= wordsPerFile; j++) {
                outFile << "word" << rand() % 100 << " "; // Write a random word
            }
            outFile.close(); // Close output file
        }
        else {
            cout << "Unable to create file " << fileName.str() << endl;
        }
    }
}

__declspec(noinline) void ParseTestFiles()
{
    unordered_map<string, int> wordFreq; // To keep track of word frequency

    std::string directory = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles"; // Directory containing input files
    std::vector<std::string> fileNames;

    // Get all file paths in the directory
    for (const auto& entry : fs::directory_iterator(directory)) 
    {
        if (entry.is_regular_file()) 
        {
            fileNames.push_back(entry.path().string());
        }
    }

    char buf[256];

    for (const auto& fileName : fileNames) 
    {
        ifstream inFile(fileName); // Open input file
        if (inFile.is_open()) 
        {
            // Read the entire file into a string.
            std::stringstream ss;
            ss << inFile.rdbuf();
            std::string fileContents = ss.str();

            const char* s = fileContents.c_str();
            while (*s != '\0')
            {
                int n = std::sscanf(s, "%255s", buf);
                if (n == 1)
                {
                    s += std::strlen(buf) + 1;
                    ++wordFreq[buf];
                }
                else
                {
                    ++s;
                }
            }
            inFile.close(); // Close input file
        }
        else 
        {
            cout << "Unable to open file " << fileName << endl;
        }
    }

    vector<pair<string, int>> wordFreqVec(wordFreq.begin(), wordFreq.end()); // Convert word frequency map to vector

    sort(wordFreqVec.begin(), wordFreqVec.end(), [](const pair<string, int>& a, const pair<string, int>& b) { // Sort vector in descending order of word frequency
        return a.second > b.second;
    });

    for (size_t i = 0; i < fileNames.size(); ++i)
    {
        string path = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\Output\\top_words_" + std::to_string(i) + ".txt";

        ofstream outFile(path); // Open output file
        if (outFile.is_open())
        {
            int count = 0;
            for (const auto& wordFreqPair : wordFreqVec)
            {
                outFile << wordFreqPair.first << ": " << wordFreqPair.second << endl; // Write top words to output file
                count++;
                if (count == 10) 
                {
                    // Limit to top 10 words
                    break;
                }
            }
            outFile.close(); // Close output file
        }
        else
        {
            cout << "Unable to open output file" << endl;
        }
    }
}

__declspec(noinline)  void ParseTestFilesConcurrent()
{
    std::string directory = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles"; // Directory containing input files
    std::vector<std::string> fileNames;

    // Get all file paths in the directory
    for (const auto& entry : fs::directory_iterator(directory))
    {
        if (entry.is_regular_file())
        {
            fileNames.push_back(entry.path().string());
        }
    }

    size_t totalThreads = 8;
    moodycamel::ConcurrentQueue <string> filePaths;
    size_t chunks = fileNames.size() / totalThreads;

    filePaths.enqueue_bulk(fileNames.begin(), fileNames.size());
    
    vector<unordered_map<string, int>> wordFreq; // To keep track of word frequency
    vector<std::thread> threads;
    threads.resize(totalThreads);

    for(size_t i = 0; i < totalThreads; ++i)
        wordFreq.emplace_back(unordered_map<string, int>());

    for (size_t i = 0; i < totalThreads; ++i)
    {
        threads[i] = std::thread([&filePaths, &chunks](unordered_map<string, int> &wordsPerThread)
        {
            char buf[256];

            vector<string> localFileNames;
            localFileNames.resize(chunks);
            filePaths.try_dequeue_bulk(localFileNames.begin(), chunks);

            for (const auto& fileName : localFileNames)
            {
                ifstream inFile(fileName); // Open input file

                if (inFile.is_open())
                {
                    // Read the entire file into a string.
                    std::stringstream ss;
                    ss << inFile.rdbuf();
                    std::string fileContents = ss.str();

                    const char* s = fileContents.c_str();
                    while (*s != '\0') 
                    {                        
                        int n = std::sscanf(s, "%255s", buf);
                        if (n == 1) 
                        {
                            s += std::strlen(buf) + 1;
                            ++wordsPerThread[buf];
                        }
                        else 
                        {
                            ++s;
                        }
                    }

                    inFile.close(); // Close input file
                }
                else
                {
                    cout << "Unable to open file " << fileName << endl;
                }
            }
        }, std::ref(wordFreq[i]));
    }

    
    for (size_t i = 0; i < threads.size(); ++i)
        threads[i].join();

    unordered_map<string, int> combinedFrequencies;
    for (size_t i = 0; i < wordFreq.size(); ++i)
    {
        for (auto& curFreq : wordFreq)
        {
            for (auto &[word, count] : curFreq)
                combinedFrequencies[word] += count;
        }
    }

    vector<pair<string, int>> wordFreqVec(combinedFrequencies.begin(), combinedFrequencies.end());

    sort(wordFreqVec.begin(), wordFreqVec.end(), [](const pair<string, int>& a, const pair<string, int>& b) { // Sort vector in descending order of word frequency
        return a.second > b.second;
    });

    vector<std::thread> outputThreads;
    outputThreads.resize(totalThreads);

    filePaths.enqueue_bulk(fileNames.begin(), fileNames.size());

    std::atomic<size_t> val = 0;
    for (size_t i = 0; i < totalThreads; ++i)
    {
        outputThreads[i] = std::thread([&filePaths, &val, &chunks](const vector<pair<string, int>> &wordFreqVec) 
        {
            for (size_t i = 0; i < chunks; ++i)
            {
                string path = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\OutputConcurrent\\top_words_" + std::to_string(val) + ".txt";
                val++;

                ofstream outFile(path); // Open output file
                if (outFile.is_open())
                {
                    int count = 0;
                    for (const auto& wordFreqPair : wordFreqVec)
                    {
                        outFile << wordFreqPair.first << ": " << wordFreqPair.second << endl; // Write top words to output file
                        count++;
                        if (count == 10)
                        {
                            // Limit to top 10 words
                            break;
                        }
                    }
                    outFile.close(); // Close output file
                }
                else
                {
                    cout << "Unable to open output file" << endl;
                }
            }
        }, std::ref(wordFreqVec));
    }

    for (size_t i = 0; i < outputThreads.size(); ++i)
        outputThreads[i].join();
}
