import seaborn as sns
import numpy as np


def plot_hours_line_graph(ax, title, hours_df):
    line_palette = {
        'Adjusted Hrs': "#1AC938",
        'Auth Hrs': "#001C7F",
        'Visit Hrs': "#D55E00",
        'PCS Visit': "#FFB482",
        'PCS Auth': "#A1C9F4",
        'Attd Visit': "#FFB482",
        'Attd Auth': "#A1C9F4",
    }
    line_dash = {
        'Adjusted Hrs': (2, 2),
        'Auth Hrs': "",
        'Visit Hrs': "",
        'PCS Visit': (8, 2),
        'PCS Auth': (8, 2),
        'Attd Visit': (2, 2),
        'Attd Auth': (2, 2),
    }

    line_style_order = ['Adjusted Hrs', 'Auth Hrs', 'Visit Hrs', 'PCS Visit', 'PCS Auth', 'Attd Visit', 'Attd Auth']

    ax = sns.lineplot(ax=ax, x='Month', y='Hours', hue='Type of Hours', style='Type of Hours', palette=line_palette,
                      dashes=line_dash, style_order=line_style_order,
                      data=hours_df.loc[hours_df['Type of Hours'] != 'Adjusted Hrs'])

    ax = sns.lineplot(ax=ax, x='Month', y='Hours', legend=False, hue='Type of Hours', style='Type of Hours',
                      palette=line_palette, dashes=line_dash, style_order=line_style_order,
                      data=hours_df.loc[hours_df['Type of Hours'] == 'Adjusted Hrs'], linewidth=4)

    ax.legend(loc='upper left')

    ax.set(title=title)
    return ax


def plot_spend_type_bar_graph(ax, spend_df):
    bar_palette = {
        'IP': "#E8000B",
        'ED': "#F14CC1",
        'NF': "#FF7C00",
        'HH': "#FFC400",
        'Pro': "#A3A3A3",
        'Out': "#12711C",
        'HCBS Respite': "#0173B2",
        'Falls': '#000000'
    }
    bar_hue_order = ['IP', 'ED', 'NF', 'HH', 'Pro', 'Out', 'HCBS Respite', 'Falls']
    # pal = sns.color_palette("Set3", 9)
    ax = sns.barplot(x='Month', y='Days of Service', hue='Event', hue_order=bar_hue_order, palette=bar_palette, ax=ax,
                     data=spend_df)
    ax.legend(loc='upper right')

    ax.set(ylim=(0, 20))
    ax.grid(False)
    return ax


def plot_dx_ddos_heatmap(ax, member):
    ax.set(title="DX Distinct Days of Service")
    dx_cols = [
        'alzh_ddos',
        'paralysis_ddos',
        'dementia_ddos',
        'stroke_ddos',
        'psychosis_ddos',
        'tbi_ddos',
        'obese_ddos',
        'oxygen_ddos',
        'hosp_bed_ddos',
        'pressure_ulcer_ddos',
        'paral_mono_ddos',
        'paral_mono_dom_ddos',
        'paral_hemi_ddos',
        'paral_hemi_dom_ddos',
        'paral_para_ddos',
        'paral_quad_ddos',
        'pulmonar_ddos',
        'copd_ddos',
        'chf_ddos',
        'heart_ddos',
        'cancer_ddos',
        'ckd_ddos',
        'esrd_ddos',
        'hyperlipid_ddos',
        'diab_ddos',
        'hypertension_ddos',
        'transplant_ddos',
        'liver_ddos',
        'depression_ddos',
        'drug_ddos',
        'alcohol_ddos',
        'hippfract_ddos'
    ]

    grid = member[['bom'] + dx_cols].set_index('bom').transpose().sort_index()
    grid.index = grid.index.map({
        'alzh_ddos': 'Alzheimers',
        'paralysis_ddos': 'Paralysis',
        'dementia_ddos': 'Dementia',
        'stroke_ddos': 'Stroke',
        'psychosis_ddos': 'Psychosis',
        'tbi_ddos': 'Tbi',
        'obese_ddos': 'Obese',
        'oxygen_ddos': 'Oxygen',
        'hosp_bed_ddos': 'Hosp Bed',
        'pressure_ulcer_ddos': 'Pressure Ulcer',
        'paral_mono_ddos': 'Paral Mono',
        'paral_mono_dom_ddos': 'Paral Mono Dom',
        'paral_hemi_ddos': 'Paral Hemi',
        'paral_hemi_dom_ddos': 'Paral Hemi Dom',
        'paral_para_ddos': 'Paral Para',
        'paral_quad_ddos': 'Paral Quad',
        'pulmonar_ddos': 'Pulmonary',
        'copd_ddos': 'COPD',
        'chf_ddos': 'CHF',
        'heart_ddos': 'Heart',
        'cancer_ddos': 'Cancer',
        'ckd_ddos': 'CKD',
        'esrd_ddos': 'ESRD',
        'hyperlipid_ddos': 'Hyperlipid',
        'diab_ddos': 'Diabetes',
        'hypertension_ddos': 'Hypertension',
        'transplant_ddos': 'Transplant',
        'liver_ddos': 'Liver',
        'depression_ddos': 'Depression',
        'drug_ddos': 'Drug',
        'alcohol_ddos': 'Alcohol',
        'hippfract_ddos': 'Hip Fracture'
    })
    grid.iloc[grid == 0] = np.nan
    sns.heatmap(grid, ax=ax, yticklabels=True, xticklabels=True, robust=True, annot=True, linewidths=.5,
                cmap="YlOrRd", cbar=False, vmin=1)
    ax.set_xlabel("Month")
    return ax


def plot_pmpm_line_graph(ax, melted_member):
    cost_cols = [
        'pmpm',
        'dme_pmpm',
        'care_attdpcs_pmpm',
        'amb_pmpm',
        'ed_pmpm',
        'ip_pmpm',
        'snf_pmpm',
        'icf_pmpm',
        'out_pmpm',
        'pro_pmpm',
        'hh_pmpm'
    ]

    pmpms = melted_member.query('variable in @cost_cols')

    pmpms = pmpms.rename(columns={"value": "Dollars"})
    pmpms['variable'] = pmpms['variable'].map({
        'pmpm': 'TC PMPM',
        'dme_pmpm': 'DME',
        'care_attdpcs_pmpm': 'Care ATTDPCS',
        'amb_pmpm': 'AMB',
        'ed_pmpm': 'ED',
        'ip_pmpm': 'IP',
        'snf_pmpm': 'SNF',
        'icf_pmpm': 'ICF',
        'out_pmpm': 'OUT',
        'pro_pmpm': 'PRO',
        'hh_pmpm': 'HH'
    })

    pmpm_palette = {
        'TC PMPM': "#0173B2",
        'DME': "#12711C",
        'Care ATTDPCS': "#8172B3",
        'AMB': '#000000',
        'ED': "#F14CC1",
        'IP': "#E8000B",
        'SNF': "#FF7C00",
        'ICF': "#FF7C00",
        'OUT': "#12711C",
        'PRO': "#A3A3A3",
        'HH': "#FFC400",
    }
    pmpm_order = [
        'TC PMPM',
        'IP',
        'ED',
        'AMB',
        'SNF',
        'ICF',
        'DME',
        'Care ATTDPCS',
        'OUT',
        'PRO',
        'HH'
    ]

    ax = sns.lineplot(ax=ax, x='Month', y='Dollars', hue='variable', palette=pmpm_palette, hue_order=pmpm_order,
                      data=pmpms.query('variable != "TC PMPM"'))
    ax.set(title="PMPMs")
    ax.set_ylabel("Cost by Category")
    ax.legend(loc='upper right')

    ax2 = ax.twinx()
    ax2 = sns.lineplot(ax=ax2, x='Month', y='Dollars', linewidth=4, alpha=.7, hue='variable', palette=pmpm_palette,
                       hue_order=pmpm_order, legend=False, data=pmpms.query('variable == "TC PMPM"'))
    ax2.set_ylabel("Total Cost")
    ax2.grid(False)
    return ax, ax2


def plot_visit_minutes_heatmap(ax, member_visits):
    visit_hrs = [f'vhr_{_pad(i)}' for i in range(1, 25)]
    visit_grid = member_visits[['good_day'] + visit_hrs].set_index('good_day')
    visit_grid = visit_grid.sort_index().transpose()
    visit_grid.index = visit_grid.index.map(lambda x: x[4:])

    ax = sns.heatmap(visit_grid, ax=ax, robust=True, annot=True, linewidths=.5, cmap="YlOrRd", vmin=1, cbar=False,
                     fmt='g')
    ax.set(title=f'Visit Minutes')
    ax.set_xlabel('Day')
    ax.set_ylabel('Hour')
    return ax


def plot_missed_minutes_heatmap(ax, member_visits):
    missed_hrs = [f'mhr_{_pad(i)}' for i in range(1, 25)]
    missed_grid = member_visits[['good_day'] + missed_hrs].set_index('good_day')
    missed_grid = missed_grid.sort_index().transpose()
    missed_grid.index = missed_grid.index.map(lambda x: x[4:])

    ax = sns.heatmap(missed_grid, ax=ax, robust=True, annot=True, linewidths=.5, cmap="YlOrRd", vmin=1, cbar=False,
                     fmt='g')  # ,
    ax.set(title=f'Missed Minutes')
    ax.set_xlabel('Day')
    ax.set_ylabel('Hour')
    return ax


def plot_auths(ax, member_auths):
    sns.set_color_codes("pastel")
    sns.barplot(x='total_hours_per_week', y='label', data=member_auths, color="b", ax=ax, label="Total")

    sns.set_color_codes("muted")
    sns.barplot(x='utilized_hours_per_week', y='label', data=member_auths, color="b", ax=ax, label="Utilized")

    for i, (up, patch) in enumerate(zip(member_auths.label, ax.patches)):
        if member_auths.total_hours_per_week.values[i] > 0:
            height = patch.get_height()
            ax.text(
                height,
                i,
                '{}%'.format(up),  # y label
                ha='left',
                va='center',
                fontweight='bold',
                size=12)

    sns.despine(left=True, bottom=True)

    ax.legend(ncol=2, loc="upper right", frameon=True)
    ax.set_xlabel('Hours Per Week')
    ax.set_ylabel(None)
    ax.set_yticklabels([])
    ax.set(title='Authorizations')
    return ax


def plot_member_small(ax, melted_member):
    plot_small_hours_line(ax, melted_member)
    plot_small_ddos_bars(ax, melted_member)


def plot_member_small_pmpms(ax, melted_member):
    plot_small_pmpm_line(ax, melted_member)
    plot_small_ddos_bars(ax, melted_member)


def plot_small_hours_line(ax, melted_member):
    h_cols = ['auth_attd_pcs_hrs', 'attd_pcs_visit_hrs']

    lines_df = melted_member[melted_member.variable.isin(h_cols)]
    auth = 'Authorized Hours'
    visit = 'Visit Hours'

    lines_df = lines_df.rename(columns={"value": "Hours", "variable": 'Type of Hours'})
    lines_df['Type of Hours'] = lines_df['Type of Hours'].map({
        'appropriate_hrs': 'Appr Hrs',
        'auth_attd_pcs_hrs': auth,
        'attd_pcs_visit_hrs': visit,
        'pcs_visit_hrs': 'PCS Visit',
        'auth_pcs_hrs': 'PCS Auth',
        'auth_attd_hrs': 'Attd Auth',
        'attd_visit_hrs': 'Attd Visit',
    })

    line_palette = {auth: "#38C976", visit: "#1767AE"}
    line_style_order = [auth, visit]

    #####
    # Hrs
    #####
    x = 'Month'
    ax = sns.lineplot(ax=ax, x=x, y='Hours', hue='Type of Hours', style='Type of Hours', palette=line_palette,
                      style_order=line_style_order, data=lines_df)

    ax.set_ylabel('Hours Per Month')
    ax.legend(loc='upper left')
    ax.grid(False)
    ax.grid(axis='y', linestyle='--')
    ax.set_ylim(bottom=0)

    for item in ([ax.title, ax.xaxis.label, ax.yaxis.label]):  # + ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(18)

    return ax


def plot_small_pmpm_line(ax, melted_member):
    melted_member = melted_member.query("variable=='care_attdpcs_pmpm'")
    care_attdpcs_pmpm = 'Attd and PCS'
    melted_member = melted_member.rename(columns={"value": "PMPM", "variable": 'Service Type'})
    melted_member['Service Type'] = melted_member['Service Type'].map({
        'care_attdpcs_pmpm': care_attdpcs_pmpm
    })

    line_palette = {care_attdpcs_pmpm: "#1767AE"}

    #####
    # Hrs
    #####
    x = 'Month'
    ax = sns.lineplot(ax=ax, x=x, y='PMPM', hue='Service Type', palette=line_palette, data=melted_member)
    ax.set_ylabel('Attd & PCS Spend Per Month')
    ax.legend(loc='upper left')
    ax.grid(False)
    ax.grid(axis='y', linestyle='--')
    ax.set_ylim(bottom=0)

    for item in ([ax.title, ax.xaxis.label, ax.yaxis.label]):  # + ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(18)

    return ax


def plot_small_ddos_bars(ax, melted_member):
    excl_bars = ['auth_attd_pcs_hrs', 'attd_pcs_visit_hrs', 'pro_ddos', 'out_ddos', 'falls_ddos',
                 'hcbs_respite_ddos']
    bars_df = melted_member[~melted_member.variable.isin(excl_bars)]
    bars_df = bars_df.rename(columns={"value": "Days of Service", 'variable': 'Event'})
    bars_df.Event = bars_df.Event.map({
        'ed_ddos': 'ED',
        'ip_ddos': 'IP',
        'nf_ddos': 'NF',
        'hh_ddos': 'HH',
        'pro_ddos': 'Pro',
        'out_ddos': 'Out',
        'hcbs_respite_ddos': 'HCBS Respite',
        'fall_ddos': 'Falls'
    })

    bar_palette = {'IP': "#CC0606", 'ED': "#BC5090", 'NF': "#EB67D0", 'HH': "#FFA600", }
    bar_hue_order = ['IP', 'ED', 'NF', 'HH', ]

    #####
    # DDOS Spend Type
    #####
    ax2 = ax.twinx()
    x = 'Month'
    ax2 = sns.barplot(x=x, y='Days of Service', hue='Event', alpha=.6, hue_order=bar_hue_order,
                      palette=bar_palette,
                      ax=ax2, data=bars_df)
    ax2.legend(loc='upper right')
    ax2.set_ylabel('Dates of Service')

    ax2.set(ylim=(0, 18))
    ax2.grid(False)

    for item in ([ax2.title, ax2.xaxis.label, ax2.yaxis.label]):
        item.set_fontsize(20)

    return ax

def _pad(i):
    if i < 10:
        return f'0{i}'
    return f'{i}'
