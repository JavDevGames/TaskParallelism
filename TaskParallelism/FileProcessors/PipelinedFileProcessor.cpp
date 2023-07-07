#include "PipelinedFileProcessor.h"

#include <filesystem>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string.h>
#include <Windows.h>
#include "../concurrentqueue.h"
#include "../Utils.h"

using namespace std;
namespace fs = std::filesystem;

__declspec(noinline) void ParseTestFilesPipelined()
{
    std::string directory = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\CreateTestFiles\\"; // Directory containing input files
    std::vector<std::string> fileNames;

    // Get all file paths in the directory
    for (const auto& entry : fs::directory_iterator(directory))
    {
        if (entry.is_regular_file())
        {
            fileNames.push_back(entry.path().string());
        }
    }

    size_t totalThreads = 4;
    moodycamel::ConcurrentQueue <string> processFileQueue;
    moodycamel::ConcurrentQueue <string> createFileQueue;
    size_t chunks = fileNames.size() / totalThreads;

    processFileQueue.enqueue_bulk(fileNames.begin(), fileNames.size());

    vector<unordered_map<string, int>> wordFreq; // To keep track of word frequency
    vector<std::thread> threads;
    threads.resize(totalThreads);

    for (size_t i = 0; i < totalThreads; ++i)
        wordFreq.emplace_back(unordered_map<string, int>());

    for (size_t i = 0; i < totalThreads; ++i)
    {
        threads[i] = std::thread([&processFileQueue, &chunks](unordered_map<string, int>& wordsPerThread)
            {
                char buf[256];

                vector<string> localFileNames;
                localFileNames.resize(chunks);
                processFileQueue.try_dequeue_bulk(localFileNames.begin(), chunks);

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
            for (auto& [word, count] : curFreq)
                combinedFrequencies[word] += count;
        }
    }

    vector<pair<string, int>> wordFreqVec(combinedFrequencies.begin(), combinedFrequencies.end());

    sort(wordFreqVec.begin(), wordFreqVec.end(), [](const pair<string, int>& a, const pair<string, int>& b) { // Sort vector in descending order of word frequency
        return a.second > b.second;
        });


    vector<std::thread> createFileThreads;
    createFileThreads.resize(totalThreads);

    std::atomic<int> curVal = -1;
    std::atomic<int> createFileQueueLength = 0;
    for (size_t i = 0; i < totalThreads; ++i)
    {
        createFileThreads[i] = std::thread([&curVal, &chunks, &createFileQueue, &createFileQueueLength](unordered_map<string, int>& wordsPerThread)
            {
                for (size_t i = 0; i < chunks; ++i)
                {
                    int nonSharedVal = curVal++;
                    string path = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesPipelined\\top_words_" + std::to_string(nonSharedVal) + ".txt";

                    HANDLE handle = CreateFileA(path.c_str(), GENERIC_WRITE | GENERIC_READ | DELETE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL); // Open output file
                    if (handle != INVALID_HANDLE_VALUE)
                    {
                        int count = 0;
                        for (const auto& wordFreqPair : wordsPerThread)
                        {
                            std::string line = wordFreqPair.first + ": " + std::to_string(wordFreqPair.second) + "\r\n";
                            DWORD bytesWritten = 0;
                            WriteFile(handle, line.c_str(), static_cast<DWORD>(line.size()), &bytesWritten, NULL); // Write top words to output file
                            count++;
                            if (count == 10)
                            {
                                // Limit to top 10 words
                                break;
                            }
                        }

                        createFileQueue.enqueue(std::move(path));
                        createFileQueueLength++;
                        CloseHandle(handle);
                    }
                    else
                    {
                        std::cout << "Unable to open output file" << std::endl;
                    }
                }
            }, std::ref(wordFreq[0]));
    }

    vector<std::thread> outputThreads;
    outputThreads.resize(totalThreads);

    std::atomic<size_t> val = 0;
    for (size_t i = 0; i < totalThreads; ++i)
    {
        outputThreads[i] = std::thread([&createFileQueue, &val, &chunks, &createFileQueueLength](const vector<pair<string, int>>& wordFreqVec)
            {
                vector<string> localFileNames;
                localFileNames.resize(chunks);

                while (createFileQueueLength > 0)
                {   
                    createFileQueue.try_dequeue_bulk(localFileNames.begin(), chunks);

                    for (size_t i = 0; i < localFileNames.size(); ++i)
                    {
                        val++;
                        string toPath = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesPipelined\\top_words_" + std::to_string(val) + ".txt";

                        std::string& fromPath = localFileNames[i];

                        // LocalFileNames is always "chunks" size
                        // but it may not have all the data, so we need to 
                        // verify that the data is there, and if not treat it as
                        // data being missing
                        if (fromPath.size() == 0)
                        {
                            continue;
                        }

                        createFileQueueLength--;


                        size_t pos = toPath.find("top_words");
                        if (pos != std::string::npos)
                        {
                            toPath.replace(pos, std::string("top_words").length(), "move_dir\\top_words");
                        }

                        std::wstring fromPathW = Utils::StringToWideString(fromPath);
                        std::wstring toPathW = Utils::StringToWideString(toPath);

                        if (!CopyFileW(fromPathW.c_str(), toPathW.c_str(), true))
                            std::cout << "Error copying file: " << GetLastError() << std::endl;
                    }
                }
            }, std::ref(wordFreqVec));
    }

    for (size_t i = 0; i < createFileThreads.size(); ++i)
        createFileThreads[i].join();
    
    for (size_t i = 0; i < outputThreads.size(); ++i)
        outputThreads[i].join();

    //cout << "Moved " << val << " files" << endl;
}
