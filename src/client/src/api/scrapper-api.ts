import axios from "axios";

// Fetch data by ID
export const fetchDataById = async (id: number) => {
  try {
    const response = await axios.get(`/api/scraped-data/${id}`);
    return response.data;
  } catch (error) {
    console.error("Error fetching data by ID:", error);
    throw error;
  }
};

// Fetch all data
export const fetchAllData = async () => {
  try {
    const response = await axios.get("/api/scraped-data");
    return response.data;
  } catch (error) {
    console.error("Error fetching all data:", error);
    throw error;
  }
};

// Save a single data entry
export const saveDataEntry = async (id: number, data: any) => {
  try {
    const response = await axios.post("/api/scraped-data", { id, data });
    return response.data;
  } catch (error) {
    console.error("Error saving data entry:", error);
    throw error;
  }
};

// Save multiple data entries
export const saveMultipleDataEntries = async (data: any[]) => {
  try {
    const response = await axios.post("/api/scraped-data/batch", data);
    return response.data;
  } catch (error) {
    console.error("Error saving multiple data entries:", error);
    throw error;
  }
};
