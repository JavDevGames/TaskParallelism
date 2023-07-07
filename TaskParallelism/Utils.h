#pragma once
#include <Windows.h>
#include <iostream>

class Utils
{
public:
	static size_t GetSectorSize()
	{
		STORAGE_PROPERTY_QUERY query;
		STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR descriptor;

		// Open the disk device
		HANDLE hDevice = CreateFileA("\\\\.\\PhysicalDrive0",
			0,                // no access to the drive
			FILE_SHARE_READ | // share mode
			FILE_SHARE_WRITE,
			NULL,             // default security attributes
			OPEN_EXISTING,    // disposition
			0,                // file attributes
			NULL);            // do not copy file attributes

		if (hDevice == INVALID_HANDLE_VALUE)
		{
			std::cerr << "Failed to open device: " << GetLastError() << std::endl;
			return -1;
		}

		// Query the disk's sector size
		query.PropertyId = StorageAccessAlignmentProperty;
		query.QueryType = PropertyStandardQuery;
		DWORD bytesReturned = 0;
		if (!DeviceIoControl(hDevice, IOCTL_STORAGE_QUERY_PROPERTY, &query, sizeof(query), &descriptor, sizeof(descriptor), &bytesReturned, NULL))
		{
			std::cerr << "Failed to query device: " << GetLastError() << std::endl;
			return -1;
		}
		CloseHandle(hDevice);

		return descriptor.BytesPerLogicalSector;
	}

	static std::wstring StringToWideString(const std::string& str)
	{
		int length = MultiByteToWideChar(CP_UTF8, 0, str.c_str(), -1, NULL, 0);
		std::wstring result(length, 0);
		MultiByteToWideChar(CP_UTF8, 0, str.c_str(), -1, &result[0], length);
		return result;
	}
};