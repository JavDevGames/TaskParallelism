#include <iostream>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <sstream>
#include <filesystem>
#include "concurrentqueue.h"

using namespace std;
namespace fs = std::filesystem;

void ParseTestFiles();
void CreateTestFiles();
void ParseTestFilesConcurrent();

int main()
{
    CreateTestFiles();
 
    //Single threaded
    //ParseTestFiles();

    //Single threaded
    ParseTestFilesConcurrent();
}

void CreateTestFiles()
{
    int numFiles = 1000; // Number of files to generate
    int wordsPerFile = 1000; // Number of words per file
    string directory = "TestFiles/"; // Directory to save files in

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

void ParseTestFiles()
{
    unordered_map<string, int> wordFreq; // To keep track of word frequency

    std::string directory = "TestFiles"; // Directory containing input files
    std::vector<std::string> fileNames;

    // Get all file paths in the directory
    for (const auto& entry : fs::directory_iterator(directory)) 
    {
        if (entry.is_regular_file()) 
        {
            fileNames.push_back(entry.path().string());
        }
    }

    for (const auto& fileName : fileNames) 
    {
        ifstream inFile(fileName); // Open input file
        if (inFile.is_open()) 
        {
            string word;
            while (inFile >> word) { // Read words
                wordFreq[word]++; // Increment word frequency
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
        string path = "Output/top_words_" + std::to_string(i) + ".txt";

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

void ParseTestFilesConcurrent()
{
    std::string directory = "TestFiles"; // Directory containing input files
    std::vector<std::string> fileNames;

    // Get all file paths in the directory
    for (const auto& entry : fs::directory_iterator(directory))
    {
        if (entry.is_regular_file())
        {
            fileNames.push_back(entry.path().string());
        }
    }

    size_t totalThreads = 2;
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
            vector<string> localFileNames;
            localFileNames.resize(chunks);
            filePaths.try_dequeue_bulk(localFileNames.begin(), chunks);

            for (const auto& fileName : localFileNames)
            {
                ifstream inFile(fileName); // Open input file
                if (inFile.is_open())
                {
                    string word;
                    while (inFile >> word)
                    {
                        // Read words
                        wordsPerThread[word]++; // Increment word frequency
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


    for (size_t i = 0; i < fileNames.size(); ++i)
    {
        string path = "Output/top_words_" + std::to_string(i) + ".txt";

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
