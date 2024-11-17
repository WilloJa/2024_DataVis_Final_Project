# Load necessary libraries
library(data.table)
library(tcltk)

# Function to get file path for input or output
get_file_path <- function(type = "input") {
  if (type == "input") {
    # Select an input file
    input_file <- tk_choose.files(caption = "Select an input file", filter = matrix(c("CSV Files", "*.csv"), ncol = 2))
    if (length(input_file) == 0 || input_file == "") {
      stop("No input file selected. Exiting...")
    }
    return(input_file)
  } else if (type == "output") {
    # Select an output directory
    output_dir <- tclvalue(tkchooseDirectory())
    if (output_dir == "") {
      stop("No output directory selected. Exiting...")
    }
    return(output_dir)
  }
}

# Definable variables
chunk_size <- 1e6  # Number of rows to read at a time
output_filename <- "steam_game_subset_2.tsv"

# Input/Output Variables
input_file <- get_file_path("input")
output_dir <- get_file_path("output")
output_file <- file.path(output_dir, output_filename)

# Verify file paths
cat("Input file path: ", input_file, "\n")
cat("Output file path: ", output_file, "\n")

# Check if the input file exists
if (!file.exists(input_file)) {
  stop("Input file does not exist: ", input_file)
}

# Define the columns to keep (names without quotation marks)
columns_to_keep <- c(
  "steamid", "personaname", "appid", "Title", "user_loccountrycode", 
  "number_of_friends", "number_of_groups"
)

# Define genre columns (the ones that will contain 1s or 0s)
genre_columns <- c(
  "Game_Genre_Action", "Game_Genre_Free_to_Play", "Game_Genre_Strategy", "Game_Genre_Adventure", 
  "Game_Genre_Indie", "Game_Genre_RPG", "Game_Genre_Animation_Modeling", 
  "Game_Genre_Video_Production", "Game_Genre_Casual", "Game_Genre_Simulation", 
  "Game_Genre_Racing", "Game_Genre_Massively_Multiplayer", "Game_Genre_Sports", 
  "Game_Genre_Early_Access", "Game_Genre_Photo_Editing", "Game_Genre_Utilities", 
  "Game_Genre_Design_Illustration", "Game_Genre_Education", 
  "Game_Genre_Software_Training", "Game_Genre_Web_Publishing", 
  "Game_Genre_Audio_Production", "Game_Genre_Accounting"
)

# Read the header to get column names
header <- fread(input_file, sep = ";", nrows = 0)
col_names <- gsub('"', "", names(header))  # Remove any quotation marks
setnames(header, col_names)  # Normalize column names

# Verify the desired columns exist
missing_columns <- setdiff(columns_to_keep, col_names)
if (length(missing_columns) > 0) {
  stop("The following columns are missing in the input file: ", paste(missing_columns, collapse = ", "))
}

# Define the output columns
output_columns <- c(columns_to_keep, "Genres")

# Process the file in chunks
chunk_start <- 1   # Starting row for the first chunk
chunk_count <- 0

while (TRUE) {
  # Read the next chunk
  chunk <- fread(
    input = input_file,
    sep = ";",
    header = FALSE,            
    skip = chunk_start,        # Skip rows already processed
    nrows = chunk_size,        # Read up to chunk_size rows
    col.names = col_names      # Use consistent column names
  )
  
  # Break the loop if no rows were read
  if (nrow(chunk) == 0) break
  
  # Create the "Genres" column by checking which genre columns are 1
  chunk[, Genres := apply(chunk[, ..genre_columns], 1, function(row) {
    genres <- names(row)[row == 1]
    if (length(genres) > 0) {
      # Remove "Game_Genre_" from each genre name
      cleaned_genres <- gsub("Game_Genre_", "", genres)
      return(paste(cleaned_genres, collapse = ", "))
    } else {
      return(NA)  # No genres selected
    }
  })]
  
  # Ensure the chunk includes "Genres" before writing (do not add "Genres" again in the header)
  chunk <- chunk[, c(columns_to_keep, "Genres"), with = FALSE]
  
  # If it's the first chunk, write the header
  if (chunk_count == 0) {
    fwrite(chunk, file = output_file, sep = "\t", col.names = TRUE)
  } else {
    # For subsequent chunks, do not include the header again
    fwrite(chunk, file = output_file, append = TRUE, sep = "\t", col.names = FALSE)
  }
  
  # Update the start of the next chunk
  chunk_start <- chunk_start + chunk_size
  chunk_count <- chunk_count + 1
  print(chunk_start)
}
