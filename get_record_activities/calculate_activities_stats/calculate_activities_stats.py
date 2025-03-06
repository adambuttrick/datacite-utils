import os
import csv
import json
import argparse
import statistics
from collections import Counter, defaultdict
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

def parse_args():
    parser = argparse.ArgumentParser(description='Calculate summary statistics for research data changes.')
    parser.add_argument('-c', '--changes', required=True, help='Path to all_changes.csv file')
    parser.add_argument('-i', '--input', required=True, help='Path to input.csv file')
    parser.add_argument('-u', '--unchanged', required=True, help='Path to unchanged_dois.csv file')
    parser.add_argument('-o', '--output', default='stats_index.csv', help='Path to output CSV file for summary statistics')
    parser.add_argument('-p', '--plots', default='charts', help='Directory to save plot images (default: charts)')
    parser.add_argument('-d', '--stats-dir', default='stats', help='Directory to save stats files (default: stats)')
    return parser.parse_args()

def load_data(changes_file, input_file, unchanged_file):
    changes_data = []
    with open(changes_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            changes_data.append(row)
    
    input_data = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            input_data.append(row)
    
    unchanged_dois = []
    with open(unchanged_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            unchanged_dois.append(row['doi'])
    
    return changes_data, input_data, unchanged_dois

def analyze_changes(changes_data):
    stats = {
        'total_changes': len(changes_data),
        'unique_dois': len(set(change['doi'] for change in changes_data)),
        'change_types': Counter(),
        'changes_by_doi': defaultdict(list),
        'action_types': Counter(),
        'timestamp_analysis': {
            'earliest': None,
            'latest': None,
            'by_month': defaultdict(int)
        }
    }
    
    for change in changes_data:
        stats['change_types'][change['change_type']] += 1
        stats['action_types'][change['action']] += 1
        stats['changes_by_doi'][change['doi']].append(change)
        
        timestamp = datetime.strptime(change['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        month_key = timestamp.strftime('%Y-%m')
        stats['timestamp_analysis']['by_month'][month_key] += 1
        
        if stats['timestamp_analysis']['earliest'] is None or timestamp < stats['timestamp_analysis']['earliest']:
            stats['timestamp_analysis']['earliest'] = timestamp
        if stats['timestamp_analysis']['latest'] is None or timestamp > stats['timestamp_analysis']['latest']:
            stats['timestamp_analysis']['latest'] = timestamp
    
    changes_per_doi = [len(changes) for doi, changes in stats['changes_by_doi'].items()]
    stats['changes_per_doi'] = {
        'mean': statistics.mean(changes_per_doi) if changes_per_doi else 0,
        'median': statistics.median(changes_per_doi) if changes_per_doi else 0,
        'min': min(changes_per_doi) if changes_per_doi else 0,
        'max': max(changes_per_doi) if changes_per_doi else 0,
        'distribution': changes_per_doi
    }
    
    if stats['timestamp_analysis']['earliest']:
        stats['timestamp_analysis']['earliest'] = stats['timestamp_analysis']['earliest'].isoformat()
    if stats['timestamp_analysis']['latest']:
        stats['timestamp_analysis']['latest'] = stats['timestamp_analysis']['latest'].isoformat()
    
    return stats

def analyze_field_changes(changes_data):
    field_stats = defaultdict(int)
    field_operations = defaultdict(lambda: defaultdict(int))
    
    for change in changes_data:
        change_type = change['change_type']
        field_stats[change_type] += 1
        
        try:
            old_val = json.loads(change['old_value']) if change['old_value'] else []
            new_val = json.loads(change['new_value']) if change['new_value'] else []
            
            if not old_val and new_val:
                operation = "addition"
            elif old_val and not new_val:
                operation = "removal"
            else:
                operation = "modification"
            
            field_operations[change_type][operation] += 1
        except (json.JSONDecodeError, TypeError):
            field_operations[change_type]["unknown"] += 1
    
    return {
        'field_counts': dict(field_stats),
        'field_operations': {field: dict(ops) for field, ops in field_operations.items()}
    }

def compare_with_input_and_unchanged(changes_data, input_data, unchanged_dois):
    changed_dois = set(change['doi'] for change in changes_data)
    input_dois = set(record['doi'] for record in input_data)
    unchanged_dois_set = set(unchanged_dois)
    
    comparison = {
        'input_dois_count': len(input_dois),
        'changed_dois_count': len(changed_dois),
        'unchanged_dois_count': len(unchanged_dois_set),
        'input_dois_with_changes': len(input_dois.intersection(changed_dois)),
        'input_dois_without_changes': len(input_dois.difference(changed_dois)),
        'changed_dois_not_in_input': len(changed_dois.difference(input_dois)),
        'unchanged_dois_in_input': len(unchanged_dois_set.intersection(input_dois)),
        'inconsistencies': []
    }
    
    for doi in unchanged_dois_set:
        if doi in changed_dois:
            comparison['inconsistencies'].append(f"DOI {doi} is in both unchanged_dois and changes_data")
    
    return comparison

def generate_charts(results, plots_dir):
    os.makedirs(plots_dir, exist_ok=True)
    
    change_types = results['changes_stats']['change_types']
    if change_types:
        plt.figure(figsize=(10, 8))
        items = sorted(change_types.items(), key=lambda x: x[1], reverse=True)
        labels = [item[0] for item in items]
        sizes = [item[1] for item in items]
        
        if len(labels) > 5:
            top_labels = labels[:5]
            top_sizes = sizes[:5]
            other_size = sum(sizes[5:])
            labels = top_labels + ['Other']
            sizes = top_sizes + [other_size]
        
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')
        plt.title('Change Types Distribution')
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'change_types_pie.png'))
        plt.close()
    
    action_types = results['changes_stats']['action_types']
    if action_types:
        plt.figure(figsize=(10, 6))
        items = sorted(action_types.items(), key=lambda x: x[1], reverse=True)
        labels = [item[0] for item in items]
        values = [item[1] for item in items]
        
        plt.bar(labels, values, color='skyblue')
        plt.xlabel('Action Type')
        plt.ylabel('Count')
        plt.title('Action Types')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'action_types_bar.png'))
        plt.close()
    
    monthly_data = results['changes_stats']['timestamp_analysis']['by_month']
    if monthly_data:
        plt.figure(figsize=(12, 6))
        months = sorted(monthly_data.keys())
        counts = [monthly_data[month] for month in months]
        
        plt.plot(months, counts, marker='o', linestyle='-', color='green')
        plt.xlabel('Month')
        plt.ylabel('Number of Changes')
        plt.title('Monthly Distribution of Changes')
        plt.xticks(rotation=45)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'monthly_distribution.png'))
        plt.close()
    
    changes_per_doi = results['changes_stats']['changes_per_doi']['distribution']
    if changes_per_doi:
        plt.figure(figsize=(10, 6))
        plt.hist(changes_per_doi, bins=min(20, max(5, len(set(changes_per_doi)))), alpha=0.7, color='purple')
        plt.xlabel('Number of Changes')
        plt.ylabel('Number of DOIs')
        plt.title('Distribution of Changes per DOI')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'changes_per_doi_histogram.png'))
        plt.close()
    
    field_operations = results['field_stats']['field_operations']
    if field_operations:
        field_counts = results['field_stats']['field_counts']
        top_fields = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)[:8]
        field_names = [field[0] for field in top_fields]
        
        all_ops = set()
        for field, ops in field_operations.items():
            all_ops.update(ops.keys())
        all_ops = sorted(all_ops)
        
        data = []
        for op in all_ops:
            op_data = []
            for field in field_names:
                if field in field_operations and op in field_operations[field]:
                    op_data.append(field_operations[field][op])
                else:
                    op_data.append(0)
            data.append(op_data)
        
        plt.figure(figsize=(12, 8))
        bottom = np.zeros(len(field_names))
        colors = plt.cm.tab10(np.linspace(0, 1, len(all_ops)))
        
        for i, op_data in enumerate(data):
            plt.bar(field_names, op_data, bottom=bottom, label=all_ops[i], color=colors[i])
            bottom += np.array(op_data)
        
        plt.xlabel('Field')
        plt.ylabel('Count')
        plt.title('Operation Types by Field')
        plt.legend(title='Operation')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'field_operations.png'))
        plt.close()
    
    comparison = results['comparison']
    if comparison:
        plt.figure(figsize=(12, 7))
        metrics = [
            'input_dois_count', 
            'changed_dois_count', 
            'unchanged_dois_count', 
            'input_dois_with_changes', 
            'input_dois_without_changes'
        ]
        labels = [
            'input DOIs', 
            'Changed DOIs', 
            'Unchanged DOIs', 
            'input DOIs with Changes', 
            'input DOIs without Changes'
        ]
        values = [comparison[metric] for metric in metrics]
        
        plt.bar(labels, values, color='teal')
        plt.xlabel('Metric')
        plt.ylabel('Count')
        plt.title('DOI Comparison')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'doi_comparison.png'))
        plt.close()
    
    return [
        os.path.join(plots_dir, 'change_types_pie.png'),
        os.path.join(plots_dir, 'action_types_bar.png'),
        os.path.join(plots_dir, 'monthly_distribution.png'),
        os.path.join(plots_dir, 'changes_per_doi_histogram.png'),
        os.path.join(plots_dir, 'field_operations.png'),
        os.path.join(plots_dir, 'doi_comparison.png')
    ]

def write_results_to_csv(results, output_file, stats_dir):
    os.makedirs(stats_dir, exist_ok=True)
    
    overview_file = os.path.join(stats_dir, "overview.csv")
    with open(overview_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Metric', 'Value'])
        writer.writerow(['Total Changes', results['changes_stats']['total_changes']])
        writer.writerow(['Unique DOIs', results['changes_stats']['unique_dois']])
        writer.writerow(['Earliest Change', results['changes_stats']['timestamp_analysis']['earliest']])
        writer.writerow(['Latest Change', results['changes_stats']['timestamp_analysis']['latest']])
        writer.writerow(['Mean Changes per DOI', f"{results['changes_stats']['changes_per_doi']['mean']:.2f}"])
        writer.writerow(['Median Changes per DOI', results['changes_stats']['changes_per_doi']['median']])
        writer.writerow(['Min Changes per DOI', results['changes_stats']['changes_per_doi']['min']])
        writer.writerow(['Max Changes per DOI', results['changes_stats']['changes_per_doi']['max']])
    
    change_types_file = os.path.join(stats_dir, "change_types.csv")
    with open(change_types_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Change Type', 'Count', 'Percentage'])
        for change_type, count in sorted(results['changes_stats']['change_types'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / results['changes_stats']['total_changes']) * 100
            writer.writerow([change_type, count, f"{percentage:.1f}%"])
    
    action_types_file = os.path.join(stats_dir, "action_types.csv")
    with open(action_types_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Action Type', 'Count', 'Percentage'])
        for action, count in sorted(results['changes_stats']['action_types'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / results['changes_stats']['total_changes']) * 100
            writer.writerow([action, count, f"{percentage:.1f}%"])
    
    monthly_file = os.path.join(stats_dir, "monthly.csv")
    with open(monthly_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Month', 'Changes'])
        for month, count in sorted(results['changes_stats']['timestamp_analysis']['by_month'].items()):
            writer.writerow([month, count])
    
    fields_file = os.path.join(stats_dir, "field_analysis.csv")
    with open(fields_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Field', 'Total Count', 'Percentage', 'Operation', 'Operation Count', 'Operation Percentage'])
        
        for field, count in sorted(results['field_stats']['field_counts'].items(), key=lambda x: x[1], reverse=True):
            field_percentage = (count / results['changes_stats']['total_changes']) * 100
            
            operations = results['field_stats']['field_operations'].get(field, {})
            
            if not operations:
                writer.writerow([field, count, f"{field_percentage:.1f}%", "", "", ""])
            else:
                first_row = True
                for op, op_count in sorted(operations.items(), key=lambda x: x[1], reverse=True):
                    op_percentage = (op_count / count) * 100
                    if first_row:
                        writer.writerow([field, count, f"{field_percentage:.1f}%", op, op_count, f"{op_percentage:.1f}%"])
                        first_row = False
                    else:
                        writer.writerow(["", "", "", op, op_count, f"{op_percentage:.1f}%"])
    
    comparison_file = os.path.join(stats_dir, "comparison.csv")
    with open(comparison_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Metric', 'Value'])
        writer.writerow(['DOIs in input', results['comparison']['input_dois_count']])
        writer.writerow(['DOIs with Changes', results['comparison']['changed_dois_count']])
        writer.writerow(['DOIs Marked as Unchanged', results['comparison']['unchanged_dois_count']])
        writer.writerow(['input DOIs with Changes', results['comparison']['input_dois_with_changes']])
        writer.writerow(['input DOIs without Changes', results['comparison']['input_dois_without_changes']])
        writer.writerow(['Changed DOIs not in input', results['comparison']['changed_dois_not_in_input']])
        writer.writerow(['Unchanged DOIs in input', results['comparison']['unchanged_dois_in_input']])
    
    if results['comparison']['inconsistencies']:
        inconsistencies_file = os.path.join(stats_dir, "inconsistencies.csv")
        with open(inconsistencies_file, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Inconsistency'])
            for inconsistency in results['comparison']['inconsistencies']:
                writer.writerow([inconsistency])
    
    index_file = output_file
    with open(index_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Report File', 'Description'])
        writer.writerow(['stats/overview.csv', 'Overview statistics'])
        writer.writerow(['stats/change_types.csv', 'Change type statistics'])
        writer.writerow(['stats/action_types.csv', 'Action type statistics'])
        writer.writerow(['stats/monthly.csv', 'Monthly distribution of changes'])
        writer.writerow(['stats/field_analysis.csv', 'Field-specific analysis'])
        writer.writerow(['stats/comparison.csv', 'Comparison with input and unchanged DOIs'])
        
        if results['comparison']['inconsistencies']:
            writer.writerow(['stats/inconsistencies.csv', 'Inconsistencies found in the data'])
    
    generated_files = [
        overview_file,
        change_types_file,
        action_types_file,
        monthly_file,
        fields_file,
        comparison_file
    ]
    
    if results['comparison']['inconsistencies']:
        generated_files.append(inconsistencies_file)
    
    return generated_files

def main():
    args = parse_args()
    changes_data, input_data, unchanged_dois = load_data(
        args.changes, args.input, args.unchanged
    )
    changes_stats = analyze_changes(changes_data)
    field_stats = analyze_field_changes(changes_data)
    comparison = compare_with_input_and_unchanged(changes_data, input_data, unchanged_dois)
    results = {
        'changes_stats': changes_stats,
        'field_stats': field_stats,
        'comparison': comparison
    }
    
    generated_files = write_results_to_csv(results, args.output, args.stats_dir)
    generated_plots = generate_charts(results, args.plots)
    
    print(f"Analysis complete. CSV files generated:")
    for file in generated_files:
        print(f"- {file}")
    
    print(f"\nCharts generated:")
    for plot in generated_plots:
        print(f"- {plot}")

if __name__ == "__main__":
    main()