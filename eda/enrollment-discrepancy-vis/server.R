shinyServer(function(input, output) {
    # sort the discrepancies according to the sliders
    discrepancies_sorted <- reactive({
        discrepancies$lbm_score <- get_lbm_score(
            discrepancies$estimated_income,
            input$lbm,
            input$lbm_sigma
        )

        income_diff <- discrepancies$income_diff
        X <- cbind(
            range01(income_diff),
            discrepancies$lbm_score,
            discrepancies$has_underreporting
        )

        w <- c(input$income_weight,
               input$lbm_weight,
               input$underreporting_weight)
        discrepancies$overall_score <- row_scores(X, w)

        discrepancies %>%
            arrange(desc(overall_score)) %>%
            filter(row_number() < input$max_rows)
    })

    selected_points <- reactive({
        brush <- input$disc_brush
        if (is.null(brush)) {
            return (discrepancies_sorted())
        }

        cur_data <- discrepancies_sorted()
        income_diff <- cur_data$income_diff
        has_underreporting <- cur_data$has_underreporting
        keep_rows <- income_diff > brush$xmin &
            income_diff < brush$xmax &
            has_underreporting > brush$ymin &
            has_underreporting < brush$ymax
        result <- cur_data[keep_rows, ]
    })

    # display the table
    new_names <- c("Home ID", "Underreporting Score", "Prob. CUIS Discrepancy.",
                   "Income Difference", "Estimated Income",
                   "Self-Reported Income", "Urban / Rural", "LBM Proximity")

    output$discrepancy_table = DT::renderDataTable({
        DT::datatable(selected_points(),
                      colnames = new_names,
                      rownames = FALSE) %>%
            formatRound('estimated_income', 2) %>%
            formatRound('income_self_reported', 2) %>%
            formatRound('has_underreporting', 2) %>%
            formatRound('income_diff', 2) %>%
            formatRound('lbm_score', 2) %>%
            formatRound('overall_score', 2)
    })

    # visualize the probabilities for selected rows (and non-income questions)
    output$disc_plot <- renderPlot({
        cur_data <- discrepancies_sorted()
        ggplot() +
            geom_point(data = cur_data,
                       aes(x = income_diff, y = has_underreporting,
                           alpha = lbm_score), size = 0.5) +
            scale_alpha(range = c(0.1, 1)) +
            labs(x = "Estimated Income - Self-Reported Income",
                 y = "Estimated Under-reporting Probability",
                 alpha = "LBM Proximity") +
            ylim(0, 1)
    })

})
