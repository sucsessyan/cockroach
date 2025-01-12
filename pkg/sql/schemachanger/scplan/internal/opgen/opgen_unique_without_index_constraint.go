// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.UniqueWithoutIndexConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.MakeAbsentUniqueWithoutIndexConstraintWriteOnly {
					return &scop.MakeAbsentUniqueWithoutIndexConstraintWriteOnly{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
						ColumnIDs:    this.ColumnIDs,
						Predicate:    this.Predicate,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.UpdateTableBackReferencesInTypes {
					if this.Predicate == nil || this.Predicate.UsesTypeIDs == nil || len(this.Predicate.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.Predicate.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.UpdateBackReferencesInSequences {
					if this.Predicate == nil || this.Predicate.UsesSequenceIDs == nil || len(this.Predicate.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateBackReferencesInSequences{
						SequenceIDs:           this.Predicate.UsesSequenceIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
			),
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.ValidateConstraint {
					return &scop.ValidateConstraint{
						TableID:              this.TableID,
						ConstraintID:         this.ConstraintID,
						IndexIDForValidation: this.IndexIDForValidation,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.MakeValidatedUniqueWithoutIndexConstraintPublic {
					return &scop.MakeValidatedUniqueWithoutIndexConstraintPublic{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.MakePublicUniqueWithoutIndexConstraintValidated {
					return &scop.MakePublicUniqueWithoutIndexConstraintValidated{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
			equiv(scpb.Status_WRITE_ONLY),
			to(scpb.Status_ABSENT,
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.RemoveUniqueWithoutIndexConstraint {
					return &scop.RemoveUniqueWithoutIndexConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
	)
}
