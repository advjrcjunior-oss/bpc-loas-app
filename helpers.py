"""
Helper functions for DOCX generation - BPC/LOAS
Pre-defined to avoid Claude regenerating them every time.
"""
import shutil
import zipfile
import re
import os
from datetime import datetime, date


def esc(t):
    """Escape XML special characters."""
    return str(t).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def run(text, bold=False, sz="24", color="000000", caps=False, italic=False, underline=False):
    """Run com Segoe UI. Aceita color para compatibilidade mas default preto."""
    rpr = '<w:rFonts w:ascii="Segoe UI" w:hAnsi="Segoe UI" w:cs="Segoe UI"/>'
    if bold:
        rpr += '<w:b/><w:bCs/>'
    if italic:
        rpr += '<w:i/><w:iCs/>'
    if caps:
        rpr += '<w:caps/>'
    if underline:
        rpr += '<w:u w:val="single"/>'
    if color and color != "000000":
        rpr += f'<w:color w:val="{color}"/>'
    rpr += f'<w:sz w:val="{sz}"/><w:szCs w:val="{sz}"/>'
    return f'<w:r><w:rPr>{rpr}</w:rPr><w:t xml:space="preserve">{esc(text)}</w:t></w:r>'


def para(runs_str, jc="both", fi=720, li=0, before=0, after=100, line=360,
         shd=None, bdr_top=False, bdr_bot=False, bdr_color="000000"):
    """Paragraph with formatting."""
    ppr = f'<w:spacing w:before="{before}" w:after="{after}" w:line="{line}" w:lineRule="auto"/>'
    ppr += f'<w:jc w:val="{jc}"/><w:ind w:firstLine="{fi}" w:left="{li}"/>'
    if shd:
        ppr += f'<w:shd w:val="clear" w:color="auto" w:fill="{shd}"/>'
    if bdr_top or bdr_bot:
        ppr += '<w:pBdr>'
        if bdr_top:
            ppr += f'<w:top w:val="single" w:sz="12" w:space="4" w:color="{bdr_color}"/>'
        if bdr_bot:
            ppr += f'<w:bottom w:val="single" w:sz="12" w:space="4" w:color="{bdr_color}"/>'
        ppr += '</w:pBdr>'
    return f'<w:p><w:pPr>{ppr}</w:pPr>{runs_str}</w:p>'


def sec_title(t):
    """Section title - caps, bold, centered, gray background, black borders."""
    return para(run(t, bold=True, sz="22", caps=True),
                jc="center", fi=0, before=160, after=160, shd="F2F2F2",
                bdr_top=True, bdr_bot=True, bdr_color="000000")


def sub_title(t):
    """Subtitle - bold, left aligned."""
    return para(run(t, bold=True, sz="22"),
                jc="left", fi=0, before=160, after=80)


def bp(text, fi=720, jc="both", before=0, after=100, **kwargs):
    """Body paragraph - accepts extra kwargs for flexibility."""
    return para(run(text), jc=jc, fi=fi, before=before, after=after)


def bp_r(runs_str, fi=720, li=0, jc="both", before=0, after=100, **kwargs):
    """Body paragraph with multiple runs (for bold names in text)."""
    return para(runs_str, jc=jc, fi=fi, li=li, before=before, after=after)


def ped(letra, texto):
    """Petition item: a) texto..."""
    return para(run(f"{letra}) ", bold=True) + run(texto),
                jc="both", fi=0, li=360, before=0, after=120)


def quesito(num, texto):
    """Questionnaire item: 1. texto..."""
    return para(run(f"{num}. ", bold=True) + run(texto),
                jc="both", fi=0, li=360, before=0, after=160)


def empty():
    """Empty line with small spacing."""
    return '<w:p><w:pPr><w:spacing w:before="80" w:after="0"/></w:pPr></w:p>'


def empty_line():
    """Empty line - alias for empty()."""
    return empty()


def table_row(cells, header=False, shd=None):
    """Generate a table row XML.
    cells: list of (text, width_pct) or just text strings
    """
    row_xml = '<w:tr>'
    if header:
        row_xml += '<w:trPr><w:tblHeader/></w:trPr>'

    for cell in cells:
        if isinstance(cell, tuple):
            text, width = cell
        else:
            text, width = cell, None

        tc_xml = '<w:tc><w:tcPr>'
        if shd:
            tc_xml += f'<w:shd w:val="clear" w:color="auto" w:fill="{shd}"/>'
        if header:
            tc_xml += '<w:shd w:val="clear" w:color="auto" w:fill="333333"/>'
        tc_xml += '</w:tcPr>'

        if header:
            tc_xml += f'<w:p><w:pPr><w:jc w:val="center"/></w:pPr>{run(text, bold=True, color="FFFFFF", sz="20")}</w:p>'
        else:
            tc_xml += f'<w:p><w:pPr><w:spacing w:before="40" w:after="40"/></w:pPr>{run(text, sz="20")}</w:p>'

        tc_xml += '</w:tc>'
        row_xml += tc_xml

    row_xml += '</w:tr>'
    return row_xml


def make_table(headers, rows, col_widths=None, total_row=None):
    """Generate a complete table XML.
    headers: list of header strings
    rows: list of lists of cell strings
    col_widths: optional list of widths in twips
    total_row: optional list of cell strings for a total/summary row
    """
    tbl = '<w:tbl><w:tblPr>'
    tbl += '<w:tblW w:w="5000" w:type="pct"/>'
    tbl += '<w:tblBorders>'
    for border in ['top', 'left', 'bottom', 'right', 'insideH', 'insideV']:
        tbl += f'<w:{border} w:val="single" w:sz="4" w:space="0" w:color="999999"/>'
    tbl += '</w:tblBorders>'
    tbl += '</w:tblPr>'

    # Header row
    tbl += table_row(headers, header=True)

    # Data rows with alternating colors
    for i, row in enumerate(rows):
        shd = "F2F2F2" if i % 2 == 0 else None
        tbl += table_row(row, shd=shd)

    # Total row (inside the table)
    if total_row:
        tbl += make_total_row(total_row)

    tbl += '</w:tbl>'
    return tbl


def make_total_row(cells):
    """Generate a total row for a table with bold text and gray background."""
    row_xml = '<w:tr>'
    for cell in cells:
        tc_xml = '<w:tc><w:tcPr>'
        tc_xml += '<w:shd w:val="clear" w:color="auto" w:fill="E8E8E8"/>'
        tc_xml += '</w:tcPr>'
        tc_xml += f'<w:p><w:pPr><w:spacing w:before="40" w:after="40"/></w:pPr>{run(cell, bold=True, sz="20")}</w:p>'
        tc_xml += '</w:tc>'
        row_xml += tc_xml
    row_xml += '</w:tr>'
    return row_xml


def setup_docx(timbrado_path, output_dir):
    """Setup base docx from timbrado template. Returns (base_dir, sect_pr, ns)."""
    base_docx = os.path.join(output_dir, '_base.docx')
    base_dir = os.path.join(output_dir, '_base_dir')

    # Clean previous base
    if os.path.isdir(base_dir):
        shutil.rmtree(base_dir, ignore_errors=True)

    shutil.copy(timbrado_path, base_docx)
    with zipfile.ZipFile(base_docx, 'r') as z:
        z.extractall(base_dir)

    # Extract sectPr and namespaces from original document
    doc_xml_path = os.path.join(base_dir, 'word', 'document.xml')
    with open(doc_xml_path, encoding='utf-8') as f:
        orig = f.read()
    sect_match = re.search(r'<w:sectPr[^>]*>.*?</w:sectPr>', orig, re.DOTALL)
    sect_pr = sect_match.group() if sect_match else ''

    # Extract ALL namespaces from the original document tag to preserve compatibility
    doc_tag_match = re.search(r'<w:document\s([^>]+)>', orig)
    if doc_tag_match:
        ns = doc_tag_match.group(1)
    else:
        ns = ('xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
              'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"')

    return base_dir, sect_pr, ns


def fix_body_xml(body_xml):
    """Fix common issues in generated body XML before saving.

    1. Unescape XML that was double-escaped inside <w:t> tags
    2. Remove empty runs
    3. Validate basic structure
    """
    def fix_escaped_xml_in_text(match):
        inner = match.group(1)
        if '&lt;w:' in inner or '&lt;/w:' in inner:
            # This text contains XML that should be actual tags - unescape it
            unescaped = (inner
                .replace('&lt;', '<')
                .replace('&gt;', '>')
                .replace('&quot;', '"')
                .replace('&amp;', '&'))
            return unescaped
        return match.group(0)

    body_xml = re.sub(
        r'<w:t[^>]*>(.*?)</w:t>',
        fix_escaped_xml_in_text,
        body_xml,
        flags=re.DOTALL
    )

    # Fix double-close runs that can result from unescaping
    body_xml = body_xml.replace('</w:r></w:r>', '</w:r>')

    # Remove empty runs (rPr with no w:t following)
    body_xml = re.sub(r'<w:r><w:rPr>[^<]*(?:</[^>]+>)*</w:rPr></w:r>', '', body_xml)

    # Fix orphaned table rows: <w:tr>...</w:tr> outside <w:tbl>
    # This happens when make_total_row() is called after make_table() instead of inside it
    # Strategy: find </w:tbl> followed by <w:tr> and move the row inside the table
    body_xml = re.sub(
        r'</w:tbl>\s*(<w:tr>.*?</w:tr>)',
        r'\1</w:tbl>',
        body_xml,
        flags=re.DOTALL
    )

    return body_xml


def save_docx(body_xml, sect_pr, ns, base_dir, output_path):
    """Save the final docx file using direct zip copy to preserve metadata."""
    # Fix common XML issues before saving
    body_xml = fix_body_xml(body_xml)

    new_doc = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
               f'<w:document {ns}><w:body>{body_xml}{sect_pr}</w:body></w:document>')

    # Find the base docx (timbrado copy) to read from directly
    base_docx = base_dir.replace('_base_dir', '_base.docx')

    # Direct zip copy method: read all files from original zip, replace only document.xml
    with zipfile.ZipFile(base_docx, 'r') as z_in:
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as z_out:
            for item in z_in.infolist():
                if item.filename == 'word/document.xml':
                    z_out.writestr(item, new_doc.encode('utf-8'))
                else:
                    z_out.writestr(item, z_in.read(item.filename))

    # Clean up base files
    if os.path.isdir(base_dir):
        shutil.rmtree(base_dir, ignore_errors=True)
    if os.path.isfile(base_docx):
        os.remove(base_docx)


def data_extenso():
    """Return current date in Portuguese format: 'Cidade, DD de mes de AAAA'."""
    meses = {1: 'janeiro', 2: 'fevereiro', 3: 'marco', 4: 'abril',
             5: 'maio', 6: 'junho', 7: 'julho', 8: 'agosto',
             9: 'setembro', 10: 'outubro', 11: 'novembro', 12: 'dezembro'}
    hoje = date.today()
    return f"{hoje.day} de {meses[hoje.month]} de {hoje.year}"


def meses_entre(data_inicio_str, data_fim=None):
    """Calculate months between DER date and now (or specified date).
    data_inicio_str: date string in DD/MM/YYYY or YYYY-MM-DD format
    """
    if data_fim is None:
        data_fim = date.today()
    elif isinstance(data_fim, str):
        try:
            data_fim = datetime.strptime(data_fim, "%Y-%m-%d").date()
        except ValueError:
            data_fim = datetime.strptime(data_fim, "%d/%m/%Y").date()

    try:
        if '-' in data_inicio_str and len(data_inicio_str) == 10 and data_inicio_str[4] == '-':
            di = datetime.strptime(data_inicio_str, "%Y-%m-%d").date()
        else:
            di = datetime.strptime(data_inicio_str, "%d/%m/%Y").date()
    except ValueError:
        return 12  # fallback

    return max(1, (data_fim.year - di.year) * 12 + (data_fim.month - di.month))
