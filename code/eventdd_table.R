# ---------------------------------------------------------
# 1. Install and load required packages if not already
# ---------------------------------------------------------
# install.packages(c("dplyr", "flextable", "webshot2"))
# webshot2::install_phantomjs() # Uncomment if PhantomJS not installed

library(dplyr)
library(flextable)
library(webshot2)

# ---------------------------------------------------------
# 2. Hard-code your regression results in a data frame
# ---------------------------------------------------------
linreg_table <- tibble(
  term         = c("lead7", "lead6", "lead5", "lead4", "lead3", 
                   "lead2", "lag0", "lag1", "lag2", "_cons"),
  coefficient  = c( 3.853,  5.591,  4.509,  1.844, -0.209,
                    -3.268,  0.631,  3.023,  2.937, 12.108),
  std_err      = c( 2.103,  2.106,  2.103,  2.100,  2.097,
                    2.097,  2.120,  2.108,  2.120,  2.090),
  t_value      = c( 1.830,  2.650,  2.140,  0.880, -0.100,
                    -1.560,  0.300,  1.430,  1.390,  5.790),
  p_value      = c( 0.067,  0.008,  0.032,  0.380,  0.920,
                    0.119,  0.766,  0.152,  0.166,  0.000),
  conf_low     = c(-0.274,  1.459,  0.383, -2.276, -4.324,
                   -7.381, -3.528, -1.112, -1.222,  8.008),
  conf_high    = c( 7.979,  9.722,  8.635,  5.964,  3.905,
                    0.846,  4.791,  7.159,  7.096, 16.209)
)

# Combine confidence interval columns into a single text column
linreg_table <- linreg_table %>%
  mutate(
    conf_interval = paste0("[", conf_low, ", ", conf_high, "]")
  ) %>%
  select(
    term,
    coefficient,
    std_err,
    t_value,
    p_value,
    conf_interval
  )

# ---------------------------------------------------------
# 3. Create a publication-quality table with flextable
# ---------------------------------------------------------
ft <- flextable(linreg_table) %>%
  set_header_labels(
    term          = "Variable",
    coefficient   = "Coefficient",
    std_err       = "Std. Err.",
    t_value       = "t",
    p_value       = "P>|t|",
    conf_interval = "95% Conf. Interval"
  ) %>%
  theme_booktabs() %>%
  autofit() %>%
  fontsize(size = 10, part = "all") %>%
  align(align = "center", part = "all")

# ---------------------------------------------------------
# 4. Save the flextable as a PNG image
# ---------------------------------------------------------
save_as_image(ft, path = "linreg_table.png")

# ---------------------------------------------------------
# Done! 
# You should now have a file named linreg_table.png in your working directory.
# ---------------------------------------------------------
