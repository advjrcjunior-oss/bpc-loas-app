from datetime import date

import pytest

from helpers import (
    bp,
    data_extenso,
    empty,
    empty_line,
    esc,
    fix_body_xml,
    make_table,
    make_total_row,
    meses_entre,
    para,
    ped,
    quesito,
    run,
    sec_title,
    sub_title,
    table_row,
)


class TestEsc:
    def test_escapes_ampersand(self):
        assert esc("Tom & Jerry") == "Tom &amp; Jerry"

    def test_escapes_angle_brackets(self):
        assert esc("<b>x</b>") == "&lt;b&gt;x&lt;/b&gt;"

    def test_escapes_quotes(self):
        assert esc('say "hi"') == "say &quot;hi&quot;"

    def test_ampersand_escaped_before_others(self):
        # If & were escaped last, "&lt;" would become "&amp;lt;"
        assert esc("<") == "&lt;"

    def test_plain_text_unchanged(self):
        assert esc("Maria da Silva") == "Maria da Silva"

    def test_coerces_non_string(self):
        assert esc(42) == "42"
        assert esc(None) == "None"


class TestRun:
    def test_default_has_font_and_size(self):
        out = run("texto")
        assert 'w:ascii="Segoe UI"' in out
        assert '<w:sz w:val="24"/>' in out
        assert "<w:t xml:space=\"preserve\">texto</w:t>" in out

    def test_bold_adds_b_tags(self):
        assert "<w:b/><w:bCs/>" in run("x", bold=True)

    def test_italic_adds_i_tags(self):
        assert "<w:i/><w:iCs/>" in run("x", italic=True)

    def test_caps_adds_caps_tag(self):
        assert "<w:caps/>" in run("x", caps=True)

    def test_underline_adds_u_tag(self):
        assert '<w:u w:val="single"/>' in run("x", underline=True)

    def test_default_black_color_omitted(self):
        assert "<w:color" not in run("x")

    def test_non_default_color_included(self):
        assert '<w:color w:val="FFFFFF"/>' in run("x", color="FFFFFF")

    def test_text_is_escaped(self):
        assert "&amp;" in run("A & B")

    def test_custom_size(self):
        assert '<w:sz w:val="20"/>' in run("x", sz="20")


class TestPara:
    def test_default_justification(self):
        assert '<w:jc w:val="both"/>' in para(run("x"))

    def test_custom_justification(self):
        assert '<w:jc w:val="center"/>' in para(run("x"), jc="center")

    def test_shading_applied(self):
        assert 'w:fill="F2F2F2"' in para(run("x"), shd="F2F2F2")

    def test_no_shading_by_default(self):
        assert "<w:shd" not in para(run("x"))

    def test_borders_omitted_by_default(self):
        assert "<w:pBdr>" not in para(run("x"))

    def test_top_border_only(self):
        out = para(run("x"), bdr_top=True)
        assert "<w:pBdr>" in out
        assert "<w:top " in out
        assert "<w:bottom " not in out

    def test_both_borders(self):
        out = para(run("x"), bdr_top=True, bdr_bot=True)
        assert "<w:top " in out
        assert "<w:bottom " in out

    def test_wraps_runs_in_paragraph(self):
        out = para("RUNS")
        assert out.startswith("<w:p>")
        assert out.endswith("</w:p>")
        assert "RUNS" in out


class TestSectionHelpers:
    def test_sec_title_is_centered_bold_caps_shaded(self):
        out = sec_title("PRELIMINARES")
        assert '<w:jc w:val="center"/>' in out
        assert "<w:b/>" in out
        assert "<w:caps/>" in out
        assert 'w:fill="F2F2F2"' in out

    def test_sub_title_is_left_aligned_bold(self):
        out = sub_title("Dos Fatos")
        assert '<w:jc w:val="left"/>' in out
        assert "<w:b/>" in out

    def test_bp_produces_body_paragraph(self):
        out = bp("Texto do corpo")
        assert "Texto do corpo" in out
        assert '<w:jc w:val="both"/>' in out

    def test_ped_prefixes_letter_in_bold(self):
        out = ped("a", "conceder o beneficio")
        assert "a) " in out
        assert "conceder o beneficio" in out
        assert "<w:b/>" in out

    def test_quesito_prefixes_number_in_bold(self):
        out = quesito(1, "Qual a doenca?")
        assert "1. " in out
        assert "Qual a doenca?" in out
        assert "<w:b/>" in out

    def test_empty_line_is_alias_of_empty(self):
        assert empty_line() == empty()

    def test_empty_produces_bare_paragraph(self):
        assert empty().startswith("<w:p>")
        assert "<w:t" not in empty()


class TestTableRow:
    def test_wraps_in_tr(self):
        out = table_row(["a", "b"])
        assert out.startswith("<w:tr>")
        assert out.endswith("</w:tr>")

    def test_one_cell_per_entry(self):
        assert table_row(["a", "b", "c"]).count("<w:tc>") == 3

    def test_header_marks_repeat_and_dark_fill(self):
        out = table_row(["Col"], header=True)
        assert "<w:tblHeader/>" in out
        assert 'w:fill="333333"' in out

    def test_header_text_is_bold_and_white(self):
        out = table_row(["Col"], header=True)
        assert "<w:b/>" in out
        assert 'w:val="FFFFFF"' in out

    def test_data_row_has_no_header_markers(self):
        out = table_row(["x"])
        assert "<w:tblHeader/>" not in out
        assert 'w:fill="333333"' not in out

    def test_shading_applied_to_data_cells(self):
        assert 'w:fill="F2F2F2"' in table_row(["x"], shd="F2F2F2")

    def test_tuple_cells_use_first_element_as_text(self):
        out = table_row([("Nome", 50)])
        assert "Nome" in out
        assert "<w:tc>" in out

    def test_cell_text_is_escaped(self):
        assert "&amp;" in table_row(["A & B"])


class TestMakeTable:
    def test_wraps_in_tbl(self):
        out = make_table(["H"], [["a"]])
        assert out.startswith("<w:tbl>")
        assert out.endswith("</w:tbl>")

    def test_includes_borders(self):
        out = make_table(["H"], [["a"]])
        for border in ["top", "left", "bottom", "right", "insideH", "insideV"]:
            assert f"<w:{border} " in out

    def test_header_plus_data_row_count(self):
        out = make_table(["H1", "H2"], [["a", "b"], ["c", "d"]])
        assert out.count("<w:tr>") == 3

    def test_alternating_shading_on_even_rows(self):
        out = make_table(["H"], [["r0"], ["r1"], ["r2"]])
        # rows 0 and 2 shaded, row 1 not
        assert out.count('w:fill="F2F2F2"') == 2

    def test_empty_rows_still_renders_header(self):
        out = make_table(["H"], [])
        assert out.count("<w:tr>") == 1

    def test_total_row_included_inside_table(self):
        out = make_table(["H"], [["a"]], total_row=["Total"])
        assert 'w:fill="E8E8E8"' in out
        assert out.index('w:fill="E8E8E8"') < out.index("</w:tbl>")

    def test_no_total_row_by_default(self):
        assert 'w:fill="E8E8E8"' not in make_table(["H"], [["a"]])


class TestMakeTotalRow:
    def test_bold_and_gray(self):
        out = make_total_row(["Total", "100"])
        assert "<w:b/>" in out
        assert 'w:fill="E8E8E8"' in out

    def test_one_cell_per_entry(self):
        assert make_total_row(["a", "b"]).count("<w:tc>") == 2


class TestFixBodyXml:
    def test_clean_xml_passes_through(self):
        clean = para(run("texto normal"))
        assert fix_body_xml(clean) == clean

    def test_unescapes_xml_tags_inside_text(self):
        broken = '<w:t xml:space="preserve">&lt;w:br/&gt;</w:t>'
        assert "<w:br/>" in fix_body_xml(broken)

    def test_leaves_ordinary_escaped_entities_alone(self):
        # No "&lt;w:" marker, so this is real content and must stay escaped
        out = fix_body_xml('<w:t xml:space="preserve">Tom &amp; Jerry</w:t>')
        assert "&amp;" in out

    def test_collapses_double_closed_runs(self):
        assert "</w:r></w:r>" not in fix_body_xml("<w:r></w:r></w:r>")

    def test_removes_run_with_bare_rpr(self):
        assert fix_body_xml("<w:r><w:rPr></w:rPr></w:r>") == ""

    def test_removes_run_whose_rpr_has_only_closing_tags(self):
        assert fix_body_xml("<w:r><w:rPr></w:b></w:rPr></w:r>") == ""

    def test_run_with_self_closing_props_is_not_stripped(self):
        # The empty-run regex only matches closing tags, so `<w:b/>` survives.
        # Documented as current behavior, not endorsed as correct.
        xml = "<w:r><w:rPr><w:b/></w:rPr></w:r>"
        assert fix_body_xml(xml) == xml

    def test_keeps_runs_that_have_text(self):
        out = fix_body_xml(run("conteudo"))
        assert "conteudo" in out
        assert "<w:r>" in out

    def test_moves_orphaned_row_inside_table(self):
        orphaned = "<w:tbl><w:tr><w:tc/></w:tr></w:tbl><w:tr><w:tc/></w:tr>"
        out = fix_body_xml(orphaned)
        assert out.endswith("</w:tbl>")
        assert out.count("</w:tbl>") == 1

    def test_orphaned_row_fix_handles_whitespace(self):
        out = fix_body_xml("<w:tbl><w:tr/></w:tbl>\n  <w:tr><w:tc/></w:tr>")
        assert out.endswith("</w:tbl>")

    def test_empty_string(self):
        assert fix_body_xml("") == ""


class TestDataExtenso:
    def test_matches_todays_date_in_portuguese(self):
        hoje = date.today()
        meses = {
            1: "janeiro", 2: "fevereiro", 3: "marco", 4: "abril",
            5: "maio", 6: "junho", 7: "julho", 8: "agosto",
            9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro",
        }
        assert data_extenso() == f"{hoje.day} de {meses[hoje.month]} de {hoje.year}"

    def test_day_is_not_zero_padded(self):
        assert not data_extenso().startswith("0")


class TestMesesEntre:
    def test_br_format_across_months(self):
        assert meses_entre("01/01/2024", "01/06/2024") == 5

    def test_iso_format_across_months(self):
        assert meses_entre("2024-01-01", "2024-06-01") == 5

    def test_accepts_date_object_as_end(self):
        assert meses_entre("01/01/2024", date(2024, 6, 1)) == 5

    def test_crosses_year_boundary(self):
        assert meses_entre("01/12/2023", "01/02/2024") == 2

    def test_spans_multiple_years(self):
        assert meses_entre("15/03/2020", "15/03/2024") == 48

    def test_same_month_clamps_to_one(self):
        assert meses_entre("01/06/2024", "15/06/2024") == 1

    def test_future_start_clamps_to_one(self):
        assert meses_entre("01/12/2024", "01/06/2024") == 1

    def test_invalid_date_returns_fallback(self):
        assert meses_entre("not-a-date", "01/06/2024") == 12

    def test_impossible_date_returns_fallback(self):
        assert meses_entre("32/13/2024", "01/06/2024") == 12

    def test_empty_string_returns_fallback(self):
        assert meses_entre("", "01/06/2024") == 12

    def test_end_date_accepts_br_string(self):
        assert meses_entre("01/01/2024", "01/03/2024") == 2

    def test_defaults_end_to_today(self):
        assert meses_entre("01/01/2020") >= 1

    @pytest.mark.parametrize("start", ["01/01/2024", "2024-01-01"])
    def test_both_input_formats_agree(self, start):
        assert meses_entre(start, "01/07/2024") == 6
