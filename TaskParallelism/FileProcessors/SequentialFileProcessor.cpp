#include "SequentialFileProcessor.h"

__declspec(noinline) void ParseTestFiles()
{
    unordered_map<string, int> wordFreq; // To keep track of word frequency

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

    std::vector<string> pathsToMove;
	for (size_t i = 0; i < fileNames.size(); ++i)
	{
		string path = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFiles\\top_words_" + std::to_string(i) + ".txt";

		HANDLE handle = CreateFileA(path.c_str(), GENERIC_WRITE | GENERIC_READ | DELETE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL); // Open output file
		if (handle != INVALID_HANDLE_VALUE)
		{
			int count = 0;
			for (const auto& wordFreqPair : wordFreqVec)
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

			pathsToMove.emplace_back(std::move(path));
			CloseHandle(handle);
		}
		else
		{
			std::cout << "Unable to open output file" << std::endl;
		}
	}

	for (size_t i = 0; i < pathsToMove.size(); ++i)
	{
		std::string& fromPath = pathsToMove[i];
		std::string toPath = fromPath;
		size_t pos = toPath.find("top_words");
		if (pos != std::string::npos)
		{
			toPath.replace(pos, std::string("top_words").length(), "move_dir\\top_words");
		}

		if (!MoveFileExA(fromPath.c_str(), toPath.c_str(), MOVEFILE_COPY_ALLOWED | MOVEFILE_REPLACE_EXISTING))
			std::cout << "Error moving file: " << GetLastError() << std::endl;
	}
}
