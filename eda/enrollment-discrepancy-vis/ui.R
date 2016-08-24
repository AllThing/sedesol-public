shinyUI(fluidPage(
    titlePanel("Enrollment App"),
    sidebarLayout(
        sidebarPanel(
            h3("Weights"),
            sliderInput("income_weight", "Income Weight", 0, 1, 1),
            sliderInput("underreporting_weight", "Underreporting Weight", 0, 1, 1),
            sliderInput("lbm_weight", "LBM Weight", 0, 1, 1),
            numericInput("lbm", "Minimum Poverty Line (LBM)", 1200),
            sliderInput("lbm_sigma", "LBM Transparency Level", 10, 200, 100, step = .5),
            numericInput("max_rows", "Maximum Points Displayed", 500),
            width = 2
        ),

        mainPanel(
            plotOutput("disc_plot", brush = brushOpts(id = "disc_brush")),
            DT::dataTableOutput("discrepancy_table")
        )
    )
))
