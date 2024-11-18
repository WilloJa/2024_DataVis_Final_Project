library(data.table)

### VARIABLES ###
data_input_file <- "E:/OneDrive - University of Utah/U of U/Graduate School/COMP5690/Project/Steam_data_sources/Steam_Games_BYU/steam_game_subset.tsv"
data_output_file <- "steamid_loc_friends.tsv"
selected_columns <- c("steamid", "personaname", "user_loccountrycode", "number_of_friends")
unique_id_column <- "steamid"  # Column to identify unique entries
batch_size <- 1e6

### FUNCTIONS ###
process_large_tsv <- function(input_file, output_file, selected_columns, id_column, batch_size = 10000) {
  # Open the input file for reading
  con <- file(input_file, open = "r")
  
  # Read and validate the header
  header <- strsplit(readLines(con, n = 1), "\t")[[1]]
  col_indices <- which(header %in% selected_columns)
  id_col_index <- which(header == id_column)
  
  if (!all(selected_columns %in% header)) {
    stop("Error: Some selected columns not found in the header. Missing columns: ", 
         paste(setdiff(selected_columns, header), collapse = ", "))
  }
  
  if (!(id_column %in% header)) {
    stop("Error: The id_column '", id_column, "' is not found in the header.")
  }
  
  # Write the header to the output file
  fwrite(
    data.table(t(header[col_indices])),
    file = output_file,
    sep = "\t",
    col.names = FALSE,
    quote = FALSE
  )
  
  # Initialize a data.table to track unique entries
  unique_ids <- data.table()
  
  # Process the file in batches
  while (length(lines <- readLines(con, n = batch_size)) > 0) {
    cat("Processing batch of size:", length(lines), "\n")
    
    # Split lines into columns and create a data.table
    batch_dt <- as.data.table(do.call(rbind, strsplit(lines, "\t")))
    
    # Handle cases where row lengths may not match the header
    if (ncol(batch_dt) != length(header)) {
      cat("Warning: Row length mismatch detected. Skipping malformed rows.\n")
      batch_dt <- batch_dt[, seq_along(header), with = FALSE]
    }
    
    setnames(batch_dt, header)
    
    # Select relevant columns
    batch_dt <- batch_dt[, ..col_indices]
    
    # Remove rows with empty or NA values
    batch_dt <- batch_dt[!apply(batch_dt, 1, function(row) any(row == "" | is.na(row)))]
    
    # Deduplicate within the batch and exclude already processed IDs
    batch_dt <- unique(batch_dt)
    batch_dt <- batch_dt[!batch_dt[[id_column]] %in% unique_ids[[id_column]]]
    
    # Update the unique ID tracking table
    if (nrow(batch_dt) > 0) {
      unique_ids <- rbindlist(list(unique_ids, batch_dt[, .(get(id_column))]), use.names = FALSE)
      
      # Write the processed batch to the output file
      fwrite(batch_dt, output_file, sep = "\t", append = TRUE, col.names = FALSE)
      cat("Wrote", nrow(batch_dt), "unique rows to the output file.\n")
    } else {
      cat("No new unique entries found in this batch.\n")
    }
  }
  
  close(con)  # Close the input file connection
  cat("Processing complete. Output saved to:", output_file, "\n")
}

### MAIN ###
process_large_tsv(data_input_file, data_output_file, selected_columns, unique_id_column, batch_size)