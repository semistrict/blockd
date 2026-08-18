use std::str::FromStr;

use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Topology {
    Independent,
    Star,
    Balanced {
        branching: usize,
    },
    Chain {
        max_depth: u32,
    },
    Mixed {
        seed: u64,
        root_ppm: u32,
        max_depth: u32,
    },
}

impl Topology {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Independent => "independent",
            Self::Star => "star",
            Self::Balanced { .. } => "balanced",
            Self::Chain { .. } => "chain",
            Self::Mixed { .. } => "mixed",
        }
    }
}

impl FromStr for Topology {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let fields = value.split(':').collect::<Vec<_>>();
        match fields.as_slice() {
            ["independent"] => Ok(Self::Independent),
            ["star"] => Ok(Self::Star),
            ["balanced", branching] => {
                let branching = parse_nonzero(branching, "balanced branching")?;
                Ok(Self::Balanced { branching })
            }
            ["chain", max_depth] => {
                let max_depth = parse_nonzero(max_depth, "chain maximum depth")?;
                Ok(Self::Chain { max_depth })
            }
            ["mixed", seed, root_ppm, max_depth] => {
                let seed = seed
                    .parse::<u64>()
                    .map_err(|error| format!("invalid mixed seed {seed}: {error}"))?;
                let root_ppm = root_ppm
                    .parse::<u32>()
                    .map_err(|error| format!("invalid mixed root share {root_ppm}: {error}"))?;
                if root_ppm > 1_000_000 {
                    return Err(format!("mixed root share exceeds one million: {root_ppm}"));
                }
                let max_depth = parse_nonzero(max_depth, "mixed maximum depth")?;
                Ok(Self::Mixed {
                    seed,
                    root_ppm,
                    max_depth,
                })
            }
            _ => Err(format!(
                "unknown provenance {value}; expected independent, star, balanced:N, chain:N, or mixed:SEED:ROOT_PPM:MAX_DEPTH"
            )),
        }
    }
}

fn parse_nonzero<T>(value: &str, field: &str) -> Result<T, String>
where
    T: FromStr + PartialEq + Default,
    T::Err: std::fmt::Display,
{
    let parsed = value
        .parse::<T>()
        .map_err(|error| format!("invalid {field} {value}: {error}"))?;
    if parsed == T::default() {
        return Err(format!("{field} must be positive"));
    }
    Ok(parsed)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VolumeProvenance {
    pub volume: u64,
    pub parent: Option<u64>,
    pub root: u64,
    pub generation: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Provenance {
    pub topology: String,
    pub volume_count: usize,
    pub roots: usize,
    pub max_generation: u32,
    pub nodes: Vec<VolumeProvenance>,
}

impl Provenance {
    pub fn build(volume_count: usize, topology: &Topology) -> Result<Self, String> {
        if volume_count == 0 {
            return Err("volume count must be positive".to_owned());
        }
        let nodes = match topology {
            Topology::Independent => independent(volume_count),
            Topology::Star => star(volume_count),
            Topology::Balanced { branching } => balanced(volume_count, *branching),
            Topology::Chain { max_depth } => chain(volume_count, *max_depth),
            Topology::Mixed {
                seed,
                root_ppm,
                max_depth,
            } => mixed(volume_count, *seed, *root_ppm, *max_depth),
        };
        let provenance = Self {
            topology: topology.name().to_owned(),
            volume_count,
            roots: nodes.iter().filter(|node| node.parent.is_none()).count(),
            max_generation: nodes.iter().map(|node| node.generation).max().unwrap_or(0),
            nodes,
        };
        provenance.validate()?;
        Ok(provenance)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.volume_count != self.nodes.len() {
            return Err(format!(
                "declared {} volumes but recorded {} provenance nodes",
                self.volume_count,
                self.nodes.len()
            ));
        }
        for (index, node) in self.nodes.iter().enumerate() {
            let expected = u64::try_from(index + 1).map_err(|error| error.to_string())?;
            if node.volume != expected {
                return Err(format!(
                    "provenance node {index} has volume {}, expected {expected}",
                    node.volume
                ));
            }
            match node.parent {
                None => {
                    if node.root != node.volume || node.generation != 0 {
                        return Err(format!(
                            "invalid root provenance for volume {}",
                            node.volume
                        ));
                    }
                }
                Some(parent) => {
                    if parent == 0 || parent >= node.volume {
                        return Err(format!(
                            "volume {} has missing, cyclic, or forward parent {parent}",
                            node.volume
                        ));
                    }
                    let parent_index = usize::try_from(parent - 1)
                        .map_err(|error| format!("parent index overflow: {error}"))?;
                    let parent_node = &self.nodes[parent_index];
                    if node.root != parent_node.root
                        || node.generation != parent_node.generation.saturating_add(1)
                    {
                        return Err(format!(
                            "volume {} does not extend parent {parent}'s provenance",
                            node.volume
                        ));
                    }
                }
            }
        }
        let roots = self
            .nodes
            .iter()
            .filter(|node| node.parent.is_none())
            .count();
        let max_generation = self
            .nodes
            .iter()
            .map(|node| node.generation)
            .max()
            .unwrap_or(0);
        if self.roots != roots || self.max_generation != max_generation {
            return Err("provenance summary does not match parent graph".to_owned());
        }
        Ok(())
    }
}

fn independent(volume_count: usize) -> Vec<VolumeProvenance> {
    (1..=volume_count)
        .map(|volume| {
            let volume = u64::try_from(volume).expect("volume count fits u64");
            VolumeProvenance {
                volume,
                parent: None,
                root: volume,
                generation: 0,
            }
        })
        .collect()
}

fn star(volume_count: usize) -> Vec<VolumeProvenance> {
    let mut nodes = Vec::with_capacity(volume_count);
    nodes.push(VolumeProvenance {
        volume: 1,
        parent: None,
        root: 1,
        generation: 0,
    });
    for volume in 2..=volume_count {
        nodes.push(VolumeProvenance {
            volume: u64::try_from(volume).expect("volume count fits u64"),
            parent: Some(1),
            root: 1,
            generation: 1,
        });
    }
    nodes
}

fn balanced(volume_count: usize, branching: usize) -> Vec<VolumeProvenance> {
    let mut nodes = Vec::with_capacity(volume_count);
    nodes.push(VolumeProvenance {
        volume: 1,
        parent: None,
        root: 1,
        generation: 0,
    });
    for index in 1..volume_count {
        let parent_index = (index - 1) / branching;
        let parent = &nodes[parent_index];
        nodes.push(VolumeProvenance {
            volume: u64::try_from(index + 1).expect("volume count fits u64"),
            parent: Some(parent.volume),
            root: parent.root,
            generation: parent.generation.saturating_add(1),
        });
    }
    nodes
}

fn chain(volume_count: usize, max_depth: u32) -> Vec<VolumeProvenance> {
    let width = usize::try_from(max_depth)
        .expect("maximum chain depth fits usize")
        .saturating_add(1);
    let mut nodes = Vec::with_capacity(volume_count);
    for index in 0..volume_count {
        let generation = u32::try_from(index % width).expect("generation fits u32");
        let volume = u64::try_from(index + 1).expect("volume count fits u64");
        if generation == 0 {
            nodes.push(VolumeProvenance {
                volume,
                parent: None,
                root: volume,
                generation,
            });
        } else {
            let parent = nodes.last().expect("chain root exists");
            nodes.push(VolumeProvenance {
                volume,
                parent: Some(parent.volume),
                root: parent.root,
                generation,
            });
        }
    }
    nodes
}

fn mixed(volume_count: usize, seed: u64, root_ppm: u32, max_depth: u32) -> Vec<VolumeProvenance> {
    let mut random = Lcg(seed);
    let mut nodes = Vec::with_capacity(volume_count);
    let mut eligible = Vec::with_capacity(volume_count);
    nodes.push(VolumeProvenance {
        volume: 1,
        parent: None,
        root: 1,
        generation: 0,
    });
    eligible.push(0usize);
    for index in 1..volume_count {
        let volume = u64::try_from(index + 1).expect("volume count fits u64");
        let make_root = random.next() % 1_000_000 < u64::from(root_ppm);
        let node = if make_root {
            VolumeProvenance {
                volume,
                parent: None,
                root: volume,
                generation: 0,
            }
        } else {
            let candidate = random.next() % u64::try_from(eligible.len()).expect("length fits");
            let parent_index = eligible[usize::try_from(candidate).expect("index fits")];
            let parent = &nodes[parent_index];
            VolumeProvenance {
                volume,
                parent: Some(parent.volume),
                root: parent.root,
                generation: parent.generation.saturating_add(1),
            }
        };
        nodes.push(node);
        if nodes[index].generation < max_depth {
            eligible.push(index);
        }
    }
    nodes
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_parser_rejects_ambiguous_or_unsafe_values() {
        assert_eq!("star".parse(), Ok(Topology::Star));
        assert!("balanced:0".parse::<Topology>().is_err());
        assert!("chain:0".parse::<Topology>().is_err());
        assert!("mixed:7:1000001:8".parse::<Topology>().is_err());
        assert!("unknown".parse::<Topology>().is_err());
    }

    #[test]
    fn fixed_count_topologies_record_exact_parent_graphs() {
        let independent = Provenance::build(5, &Topology::Independent).expect("independent");
        assert_eq!(independent.roots, 5);
        assert_eq!(independent.max_generation, 0);

        let star = Provenance::build(5, &Topology::Star).expect("star");
        assert_eq!(star.roots, 1);
        assert_eq!(star.max_generation, 1);
        assert!(star.nodes[1..].iter().all(|node| node.parent == Some(1)));

        let balanced =
            Provenance::build(7, &Topology::Balanced { branching: 2 }).expect("balanced tree");
        assert_eq!(balanced.max_generation, 2);
        assert_eq!(balanced.nodes[6].parent, Some(3));

        let chains = Provenance::build(8, &Topology::Chain { max_depth: 2 }).expect("chains");
        assert_eq!(chains.roots, 3);
        assert_eq!(chains.max_generation, 2);
        assert_eq!(chains.nodes[3].parent, None);
    }

    #[test]
    fn mixed_provenance_is_deterministic_and_valid() {
        let topology = Topology::Mixed {
            seed: 17,
            root_ppm: 100_000,
            max_depth: 4,
        };
        let first = Provenance::build(1_000, &topology).expect("mixed fleet");
        let second = Provenance::build(1_000, &topology).expect("mixed fleet");
        assert_eq!(first, second);
        assert!(first.roots > 1);
        assert!(first.max_generation <= 4);
        first.validate().expect("valid explicit parent graph");
    }
}
